from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from google.api_core.exceptions import NotFound
from google.cloud.bigquery import LoadJobConfig, WriteDisposition
from sqlalchemy import create_engine, text
from datetime import datetime
import pandas as pd
import logging
import time

# ============================================================
# KONFIGURACJA
# ============================================================
MARIADB_CONN_ID = "mariadb_appwrite"
SSH_CONN_ID     = "vps_ssh"
REMOTE_HOST     = Variable.get("REMOTE_HOST")
REMOTE_PORT     = 3306
GCP_CONN_ID     = "google_cloud"
BQ_PROJECT      = "genistry-379120"
BQ_DATASET      = "RAW"
# ============================================================

CAST_TO_STRING = {
    "_1_database_1_collection_1": [
        "_permissions", "Photos", "Persons",
        "personsTAGS", "yearTAGS", "AdditionalInfo",
        "WebLink", "ThumbnailPhoto", "extraTags",
    ],
    "_1_database_1_collection_2": [
        "_permissions", "FamilyInfo",
    ],
}

# Kolumny każdej tabeli do MERGE (bez GeoLocalization która jest pomijana)
COLUMNS = {
    "GENISTREE_OBJECTS": [
        "_id", "_uid", "_createdAt", "_updatedAt", "_permissions",
        "CustomDocumentID", "CreateUserID", "ValidateUserID", "Place",
        "Photos", "Persons", "Address", "Year", "personsTAGS", "yearTAGS",
        "Status", "CustomDocumentTypeID", "AdditionalInfo", "GeoLat", "GeoLon",
        "WebLink", "ThumbnailPhoto", "Signature", "extraTags",
    ],
    "GENISTREE_CENSUS": [
        "_id", "_uid", "_createdAt", "_updatedAt", "_permissions",
        "Place", "WebLink", "CreateUserID", "GeoLat", "GeoLon",
        "Description", "Owner", "Region", "Signature", "FamilyInfo",
        "Year", "revisionBooks", "PageNumber", "PageNumberOriginal",
    ],
}


def _wait_for_connection(engine, retries=8, delay=0.5):
    """
    Aktywnie czeka na gotowość tunelu SSH zamiast ślepego sleep().
    Próbuje połączyć się co `delay` sekund, max `retries` razy.
    """
    log = logging.getLogger(__name__)
    for i in range(retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception:
            log.debug(f"Tunel nie gotowy, próba {i + 1}/{retries} — czekam {delay}s")
            time.sleep(delay)
    raise RuntimeError("Nie można połączyć się przez tunel SSH po wielu próbach")


def get_max_updated_at(bq_table: str) -> str | None:
    """
    Pobiera MAX(_updatedAt) z tabeli RAW w BigQuery.
    Zwraca None jeśli tabela jest pusta lub nie istnieje (pierwsze uruchomienie).
    """
    log = logging.getLogger(__name__)
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    client  = bq_hook.get_client(project_id=BQ_PROJECT)

    try:
        result = client.query(f"""
            SELECT MAX(_updatedAt) AS max_updated
            FROM `{BQ_PROJECT}.{BQ_DATASET}.{bq_table}`
        """).result()
        row = next(iter(result), None)
        if row and row["max_updated"]:
            max_dt = row["max_updated"].isoformat()
            log.info(f"MAX(_updatedAt) dla {bq_table}: {max_dt}")
            return max_dt
    except Exception as e:
        log.warning(f"Nie można odczytać MAX(_updatedAt) z {bq_table}: {e}")

    log.info(f"Brak danych w {bq_table} — full load")
    return None


def load_to_staging(bq_table: str, mysql_table: str, local_port: int, **context):
    """
    Pobiera dane z MariaDB przez tunel SSH i ładuje do tabeli STAGING.
    Przy pierwszym uruchomieniu (RAW pusty) → pobiera wszystko.
    Przy kolejnych → tylko rekordy nowsze niż MAX(_updatedAt) w RAW.
    Zawsze nadpisuje STAGING (WRITE_TRUNCATE).
    """
    log = logging.getLogger(__name__)

    # 1. Sprawdź do kiedy mamy dane w RAW
    max_updated_at = get_max_updated_at(bq_table)

    if max_updated_at:
        sql = f"""
            SELECT * FROM `{mysql_table}`
            WHERE _updatedAt > '{max_updated_at}'
        """
        log.info(f"Incremental load od {max_updated_at}")
    else:
        sql = f"SELECT * FROM `{mysql_table}`"
        log.info("Full load — pierwsze uruchomienie")

    # 2. Pobierz credentials
    db_conn  = BaseHook.get_connection(MARIADB_CONN_ID)
    ssh_hook = SSHHook(ssh_conn_id=SSH_CONN_ID)

    # 3. Otwórz tunel SSH i pobierz dane
    log.info(f"Otwieranie tunelu SSH → {REMOTE_HOST}:{REMOTE_PORT} (local:{local_port})")
    with ssh_hook.get_tunnel(
        remote_port=REMOTE_PORT,
        remote_host=REMOTE_HOST,
        local_port=local_port,
    ) as tunnel:
        tunnel.start()

        engine = create_engine(
            f"mysql+pymysql://{db_conn.login}:{db_conn.password}"
            f"@127.0.0.1:{local_port}/{db_conn.schema}"
        )

        # Aktywne czekanie na gotowość tunelu zamiast time.sleep(2)
        _wait_for_connection(engine)

        try:
            log.info(f"Pobieranie danych z: {mysql_table}")
            df = pd.read_sql(sql, engine)
            log.info(f"Pobrano {len(df)} rekordów")
        finally:
            engine.dispose()

    if df.empty:
        log.info("Brak nowych rekordów — STAGING będzie pusty.")
        # Zapisz info do XCom żeby merge wiedział że nie ma co robić
        context["ti"].xcom_push(key=f"has_data_{bq_table}", value=False)
        return

    context["ti"].xcom_push(key=f"has_data_{bq_table}", value=True)

    # 4. Konwersja typów
    df = df.drop(columns=["GeoLocalization"], errors="ignore")

    cast_cols = CAST_TO_STRING.get(mysql_table, [])
    for col in cast_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).where(df[col].notna(), other=None)

    for col in ["_createdAt", "_updatedAt"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # 5. Załaduj do STAGING (zawsze WRITE_TRUNCATE)
    staging_table = f"{BQ_PROJECT}.{BQ_DATASET}.{bq_table}_STAGING"
    log.info(f"Ładowanie do STAGING: {staging_table}")

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    client  = bq_hook.get_client(project_id=BQ_PROJECT)

    job_config = LoadJobConfig(
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, staging_table, job_config=job_config)
    job.result()
    log.info(f"STAGING gotowy: {len(df)} rekordów")


def merge_to_raw(bq_table: str, staging_task_id: str, **context):
    """
    Wykonuje MERGE z STAGING do RAW używając _uid jako klucza.
    - MATCHED + _updatedAt różny → UPDATE
    - NOT MATCHED BY TARGET → INSERT
    - NOT MATCHED BY SOURCE → DELETE (rekord usunięty w MariaDB)

    Gdy tabela RAW jeszcze nie istnieje, tworzy ją jako kopię STAGING
    (pierwszy run — jak merge_osm_to_raw w dag_osm_shrines.py).

    Pomija MERGE jeśli STAGING jest pusty.
    """
    log = logging.getLogger(__name__)

    # Sprawdź przez XCom czy staging task miał dane
    has_data = context["ti"].xcom_pull(
        task_ids=staging_task_id,
        key=f"has_data_{bq_table}"
    )
    if not has_data:
        log.info(f"Brak nowych danych w STAGING dla {bq_table} — pomijam MERGE.")
        return

    target_fq = f"{BQ_PROJECT}.{BQ_DATASET}.{bq_table}"
    staging_fq = f"{BQ_PROJECT}.{BQ_DATASET}.{bq_table}_STAGING"

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    client = bq_hook.get_client(project_id=BQ_PROJECT)

    try:
        client.get_table(target_fq)
    except NotFound:
        log.info(
            f"Tabela {bq_table} nie istnieje — CREATE TABLE AS SELECT z STAGING (pierwszy run)"
        )
        client.query(f"""
            CREATE TABLE `{target_fq}` AS
            SELECT * FROM `{staging_fq}`
        """).result()
        log.info(f"Utworzono RAW.{bq_table} ze STAGING — pomijam MERGE w tym runie.")
        return

    cols = COLUMNS[bq_table]
    target = f"`{target_fq}`"
    staging = f"`{staging_fq}`"

    # Buduj listę kolumn do UPDATE (wszystkie oprócz _uid i _createdAt)
    update_cols = [
        c for c in cols if c not in ("_uid", "_createdAt")
    ]
    update_set = ",\n        ".join(
        [f"target.{c} = source.{c}" for c in update_cols]
    )

    # Buduj listę kolumn do INSERT
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"source.{c}" for c in cols])

    merge_sql = f"""
        MERGE {target} AS target
        USING {staging} AS source
        ON target._uid = source._uid

        WHEN MATCHED AND target._updatedAt != source._updatedAt THEN
            UPDATE SET
                {update_set}

        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})

    """

    log.info(f"Wykonuję MERGE dla {bq_table}")
    client.query(merge_sql).result()
    log.info(f"MERGE zakończony dla {bq_table}")


with DAG(
    dag_id="extract_mariadb_to_bigquery_raw",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["genealogy", "raw", "bigquery"],
    doc_md="""
    ## Extract MariaDB → BigQuery RAW (STAGING + MERGE)

    Incremental load z MariaDB do BigQuery przez warstwę STAGING.

    Flow:
    1. Pobierz MAX(_updatedAt) z RAW
    2. Załaduj nowe/zmienione rekordy do STAGING (WRITE_TRUNCATE)
    3. MERGE STAGING → RAW po kluczu _uid:
       - zmieniony rekord → UPDATE
       - nowy rekord → INSERT
       - usunięty rekord → DELETE
       Pierwszy run: brak tabeli RAW → CREATE TABLE … AS SELECT ze STAGING (bez MERGE).

    Tabele:
    - RAW.GENISTREE_OBJECTS + RAW.GENISTREE_OBJECTS_STAGING  ← _1_database_1_collection_1
    - RAW.GENISTREE_CENSUS + RAW.GENISTREE_CENSUS_STAGING  ← _1_database_1_collection_2
    """
) as dag:

    # --- GENISTREE_OBJECTS (dokumenty z aplikacji Genistree) ---
    stage_genistree_objects = PythonOperator(
        task_id="stage_genistree_objects",
        python_callable=load_to_staging,
        op_kwargs={
            "bq_table": "GENISTREE_OBJECTS",
            "mysql_table": "_1_database_1_collection_1",
            "local_port": 3307,
        },
    )

    merge_genistree_objects = PythonOperator(
        task_id="merge_genistree_objects",
        python_callable=merge_to_raw,
        op_kwargs={
            "bq_table": "GENISTREE_OBJECTS",
            "staging_task_id": "stage_genistree_objects",
        },
    )

    # --- GENISTREE_CENSUS (spisy rewizyjne) ---
    stage_genistree_census = PythonOperator(
        task_id="stage_genistree_census",
        python_callable=load_to_staging,
        op_kwargs={
            "bq_table": "GENISTREE_CENSUS",
            "mysql_table": "_1_database_1_collection_2",
            "local_port": 3308,
        },
    )

    merge_genistree_census = PythonOperator(
        task_id="merge_genistree_census",
        python_callable=merge_to_raw,
        op_kwargs={
            "bq_table": "GENISTREE_CENSUS",
            "staging_task_id": "stage_genistree_census",
        },
    )

    # Flow: staging równolegle, potem merge równolegle
    stage_genistree_objects >> merge_genistree_objects
    stage_genistree_census >> merge_genistree_census