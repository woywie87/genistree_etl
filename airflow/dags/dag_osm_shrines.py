from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.email import send_email
from google.api_core.exceptions import NotFound
from google.cloud.bigquery import LoadJobConfig, WriteDisposition
from datetime import datetime, timezone
import os
import pandas as pd
import requests
import logging
import time

# ============================================================
# KONFIGURACJA
# ============================================================
GCP_CONN_ID = "google_cloud"
BQ_PROJECT  = "genistry-379120"
BQ_DATASET  = "RAW"
# Docelowa tabela RAW (wynik MERGE ze STAGING).
BQ_TABLE = "OSM_OBJECTS"
# Partia z bieżącego runu (pełna przy pierwszym syncu, inkrement po `newer`) — WRITE_TRUNCATE.
BQ_STAGING_TABLE = f"{BQ_TABLE}_STAGING"

# Tabela pomocnicza przechowująca znaczniki czasu ostatniego pobrania.
# Dzięki temu każdy kolejny run DAG-a odpytuje Overpass tylko o elementy
# zmienione/dodane od poprzedniego uruchomienia (filtr `newer`).
BQ_META_TABLE = "OSM_OBJECTS_META"

# Kolumny zgodne z wierszami z `fetch_overpass` — używane w MERGE STAGING → RAW.
OSM_OBJECT_COLUMNS = [
    "osm_id",
    "osm_type",
    "historic_type",
    "lat",
    "lon",
    "name",
    "material",
    "start_date",
    "description",
    "voivodeship",
    "fetched_at",
]

# Adresat powiadomień e-mail wysyłanych po pomyślnym załadowaniu danych.
# Ustaw w airflow/.env: OSM_SHRINES_NOTIFY_EMAIL=...
# `send_email` (SMTP) wymaga konfiguracji w airflow/.env / airflow.cfg:
#   AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
#   AIRFLOW__SMTP__SMTP_PORT=587
#   AIRFLOW__SMTP__SMTP_STARTTLS=True
#   AIRFLOW__SMTP__SMTP_USER=...
#   AIRFLOW__SMTP__SMTP_PASSWORD=...   # hasło aplikacji (Gmail), nie hasło konta
#   AIRFLOW__SMTP__SMTP_MAIL_FROM=...
NOTIFY_EMAIL = (os.getenv("OSM_SHRINES_NOTIFY_EMAIL") or "").strip() or "twoj@gmail.com"
# ============================================================

VOIVODESHIPS = {
     "swietokrzyskie":     (50.2, 19.8, 51.4, 21.6),
    # "dolnoslaskie":       (50.2, 14.9, 51.9, 17.9),
    # "kujawsko_pomorskie": (52.4, 17.2, 53.8, 19.7),
    # "lubelskie":          (50.4, 21.8, 52.0, 24.2),
    # "lubuskie":           (51.3, 14.6, 52.9, 16.3),
    # "lodzkie":            (51.0, 18.1, 52.4, 20.6),
     "malopolskie":        (49.2, 19.1, 50.5, 21.5),
    # "mazowieckie":        (51.0, 19.4, 53.5, 22.8),
    # "opolskie":           (50.2, 17.0, 51.1, 18.7),
    # "podkarpackie":       (49.2, 21.6, 50.6, 24.1),
    # "podlaskie":          (52.4, 22.0, 54.5, 24.1),
    # "pomorskie":          (53.5, 16.8, 54.9, 19.4),
    # "slaskie":            (49.5, 17.9, 50.8, 19.7),
    # "warminsko_mazurskie":(53.4, 19.1, 54.5, 22.9),
    # "wielkopolskie":      (51.2, 15.9, 53.6, 19.1),
    # "zachodniopomorskie": (53.0, 13.9, 54.5, 16.9),
}

OVERPASS_URL = (os.getenv("OVERPASS_URL") or "https://overpass-api.de/api/interpreter").strip()
# Limit czasu zapytania po stronie Overpass oraz oczekiwanie HTTP (read musi być >= timeout QL + transfer).
OVERPASS_QL_TIMEOUT_S = 300
OVERPASS_HTTP_TIMEOUT_S = 420
# Publiczny Overpass często zwraca 502/503/504 przy obciążeniu — kilka prób z backoffem.
OVERPASS_HTTP_ATTEMPTS = max(1, int(os.getenv("OVERPASS_HTTP_ATTEMPTS", "4")))
OVERPASS_RETRY_SLEEP_S = max(5, int(os.getenv("OVERPASS_RETRY_SLEEP_S", "20")))

# overpass-api.de zwraca 406, jeśli User-Agent jest zbyt ogólny (np. domyślny
# python-requests). Skrypty muszą się identyfikować — patrz:
# https://github.com/drolbr/Overpass-API/issues/791
# User-Agent musi być w ASCII — nagłówki HTTP są kodowane jako latin-1
# (znak Unicode np. "→" powoduje UnicodeEncodeError w urllib3/requests).
OVERPASS_HEADERS = {
    "User-Agent": (
        "genistree_etl-airflow/1.0 "
        "(DAG fetch_osm_shrines_to_bigquery; OSM wayside_cross/shrine to RAW.OSM_OBJECTS via STAGING)"
    ),
}

# Zapytanie Overpass QL z filtrem `newer`.
#
# Filtr `newer:"<timestamp>"` zwraca wyłącznie elementy, których wersja
# w bazie OSM jest nowsza niż podana data (UTC, format ISO-8601).
# Dzięki temu przy każdym codziennym uruchomieniu pobieramy tylko różnicę
# względem poprzedniego runu — nie ściągamy całości od nowa.
#
# Odpowiedź JSON zawiera pole `osm3s.timestamp_osm_base` — jest to czas,
# do którego baza Overpass jest aktualna. Zapisujemy go w BQ_META_TABLE
# i używamy jako wartości `newer` w następnym uruchomieniu.
#
# Gdy tabela META nie istnieje (pierwszy run), newer_since = None
# i poniższy blok `newer` jest pomijany — pobieramy wtedy pełny snapshot.
OVERPASS_QUERY_FULL = """
[out:json][timeout:{timeout_s}];
(
  node["historic"="wayside_cross"]({south},{west},{north},{east});
  node["historic"="wayside_shrine"]({south},{west},{north},{east});
);
out body;
"""

OVERPASS_QUERY_INCREMENTAL = """
[out:json][timeout:{timeout_s}];
(
  node["historic"="wayside_cross"](newer:"{newer_since}")({south},{west},{north},{east});
  node["historic"="wayside_shrine"](newer:"{newer_since}")({south},{west},{north},{east});
);
out body;
"""


# ---------------------------------------------------------------------------
# Krok 1 (pomocniczy): odczytaj znacznik czasu ostatniego pobrania z BQ
# ---------------------------------------------------------------------------
def get_last_sync_timestamp(**context) -> str | None:
    """
    Pobiera z tabeli BQ_META_TABLE znacznik czasu (osm_base_timestamp)
    ostatniego pomyślnego runu. Wartość ta posłuży jako dolna granica
    filtru `newer` w zapytaniach Overpass.

    Zwraca None, jeśli tabela nie istnieje lub jest pusta — sygnał do
    wykonania pełnego (nie-inkrementalnego) pobrania.
    """
    log = logging.getLogger(__name__)
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    client  = bq_hook.get_client(project_id=BQ_PROJECT)

    try:
        result = client.query(f"""
            SELECT osm_base_timestamp
            FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_META_TABLE}`
            ORDER BY run_at DESC
            LIMIT 1
        """).result()

        rows = list(result)
        if rows:
            ts = rows[0]["osm_base_timestamp"]
            log.info(f"Ostatni sync: {ts} — tryb INKREMENTALNY")
            context["ti"].xcom_push(key="newer_since", value=ts)
            return ts
        else:
            log.info("META pusta — tryb PEŁNY (pierwszy run)")
            context["ti"].xcom_push(key="newer_since", value=None)
            return None

    except Exception as e:
        log.warning(f"Tabela META nie istnieje lub błąd odczytu: {e}")
        log.info("Tryb PEŁNY (pierwszy run)")
        context["ti"].xcom_push(key="newer_since", value=None)
        return None


# ---------------------------------------------------------------------------
# Krok 2: pobieranie danych z Overpass dla jednego województwa
# ---------------------------------------------------------------------------
def fetch_overpass(voivodeship: str, bbox: tuple, **context):
    """
    Wysyła zapytanie do Overpass API dla podanego województwa.

    Jeśli `newer_since` (z XCom poprzedniego taska) jest ustawione,
    używa zapytania inkrementalnego (filtr `newer`) i pobiera tylko
    elementy zmienione od tamtej daty.

    W przeciwnym razie pobiera pełny snapshot całego obszaru.

    Wynik (lista słowników) i timestamp bazy Overpass (`osm_base_timestamp`)
    są przekazywane przez XCom do kolejnych tasków.
    """
    log = logging.getLogger(__name__)
    south, west, north, east = bbox
    ti = context["ti"]

    # Odczytaj znacznik czasu z taska get_last_sync_timestamp
    newer_since = ti.xcom_pull(task_ids="get_last_sync_timestamp", key="newer_since")

    if newer_since:
        log.info(f"[{voivodeship}] Tryb INKREMENTALNY — newer: {newer_since}")
        query = OVERPASS_QUERY_INCREMENTAL.format(
            timeout_s=OVERPASS_QL_TIMEOUT_S,
            newer_since=newer_since,
            south=south, west=west, north=north, east=east,
        )
    else:
        log.info(f"[{voivodeship}] Tryb PEŁNY — brak poprzedniego timestampa")
        query = OVERPASS_QUERY_FULL.format(
            timeout_s=OVERPASS_QL_TIMEOUT_S,
            south=south, west=west, north=north, east=east,
        )

    # Krótka przerwa między zapytaniami — dobre praktyki dla publicznych
    # instancji Overpass (unikamy throttlingu / bana IP)
    time.sleep(2)

    response = None
    for attempt in range(1, OVERPASS_HTTP_ATTEMPTS + 1):
        try:
            response = requests.post(
                OVERPASS_URL,
                data={"data": query},
                headers=OVERPASS_HEADERS,
                timeout=OVERPASS_HTTP_TIMEOUT_S,
            )
            response.raise_for_status()
            break
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            transient = code in (429, 502, 503, 504)
            if transient and attempt < OVERPASS_HTTP_ATTEMPTS:
                wait = OVERPASS_RETRY_SLEEP_S * (2 ** (attempt - 1))
                log.warning(
                    f"[{voivodeship}] Overpass HTTP {code} (próba {attempt}/{OVERPASS_HTTP_ATTEMPTS}), "
                    f"czekam {wait}s przed ponowieniem"
                )
                time.sleep(wait)
                continue
            raise
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < OVERPASS_HTTP_ATTEMPTS:
                wait = OVERPASS_RETRY_SLEEP_S * (2 ** (attempt - 1))
                log.warning(
                    f"[{voivodeship}] Overpass sieć/timeout: {e!r} (próba {attempt}/{OVERPASS_HTTP_ATTEMPTS}), "
                    f"czekam {wait}s przed ponowieniem"
                )
                time.sleep(wait)
                continue
            raise

    assert response is not None

    payload = response.json()
    elements = payload.get("elements", [])

    # `timestamp_osm_base` to czas, do którego baza Overpass jest aktualna.
    # Zapisujemy go, aby użyć jako wartości `newer` w następnym runie.
    osm_base_timestamp = payload.get("osm3s", {}).get("timestamp_osm_base")
    log.info(f"[{voivodeship}] Znaleziono {len(elements)} obiektów | osm_base: {osm_base_timestamp}")

    rows = []
    for el in elements:
        tags = el.get("tags", {})
        rows.append({
            "osm_id":            el.get("id"),
            "osm_type":          el.get("type"),
            "historic_type":     tags.get("historic"),
            "lat":               el.get("lat"),
            "lon":               el.get("lon"),
            "name":              tags.get("name"),
            "material":          tags.get("material"),
            "start_date":        tags.get("start_date"),
            "description":       tags.get("description"),
            "voivodeship":       voivodeship,
            "fetched_at":        datetime.now(timezone.utc).isoformat(),
        })

    ti.xcom_push(key=f"osm_{voivodeship}", value=rows)

    # Zapisz osm_base_timestamp — finalnie użyjemy najnowszego spośród
    # wszystkich województw (obsługa w save_sync_timestamp)
    ti.xcom_push(key=f"osm_base_ts_{voivodeship}", value=osm_base_timestamp)


# ---------------------------------------------------------------------------
# Krok 3: sprawdź, czy są nowe/zmienione dane
# ---------------------------------------------------------------------------
def check_new_data(**context):
    """
    Zbiera wyniki ze wszystkich tasków fetch_* — to jest partia do STAGING
    (pełny obszar przy pierwszym runie bez META, albo wyłącznie zmiany
    z filtru Overpass `newer` przy kolejnych).

    Dodatkowo zlicza rekordy per województwo i zapisuje statystyki do XCom
    — zostaną użyte w treści maila powiadomienia.

    Zwraca nazwę kolejnego taska (branching):
      - "osm_to_staging" — gdy Overpass zwrócił jakiekolwiek elementy
      - "skip"           — gdy brak elementów w tej partii
    """
    log = logging.getLogger(__name__)
    ti = context["ti"]

    all_rows: list = []
    # Zliczanie per województwo — trafi do maila powiadomienia
    counts_per_voivodeship: dict[str, int] = {}

    for voivodeship in VOIVODESHIPS.keys():
        rows = ti.xcom_pull(
            task_ids=f"fetch_{voivodeship}",
            key=f"osm_{voivodeship}"
        ) or []
        counts_per_voivodeship[voivodeship] = len(rows)
        all_rows.extend(rows)

    total = len(all_rows)
    log.info(f"Obiektów w partii do STAGING (z Overpass): {total}")

    if all_rows:
        ti.xcom_push(key="staging_rows", value=all_rows)
        # Statystyki dla maila: łączna liczba + rozbicie per województwo
        ti.xcom_push(key="staging_count", value=total)
        ti.xcom_push(key="counts_per_voivodeship", value=counts_per_voivodeship)
        return "osm_to_staging"
    else:
        return "skip"


# ---------------------------------------------------------------------------
# Krok 4a: załaduj partię do STAGING (jak GRAVES_STAGING w bigquery.py)
# ---------------------------------------------------------------------------
def load_osm_to_staging(**context):
    """
    Ładuje partię z XCom (`staging_rows`) do OSM_OBJECTS_STAGING w BigQuery.
    Zawsze WRITE_TRUNCATE — w STAGING jest wyłącznie bieżący batch
    (pierwszy pełny load albo inkrement z Overpass).
    """
    log = logging.getLogger(__name__)
    ti = context["ti"]

    staging_rows = ti.xcom_pull(task_ids="check_new_data", key="staging_rows") or []

    if not staging_rows:
        log.info("Brak wierszy w XCom — nic do załadowania do STAGING.")
        return

    df = pd.DataFrame(staging_rows)
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    if "fetched_at" in df.columns:
        df["fetched_at"] = pd.to_datetime(df["fetched_at"], errors="coerce", utc=True)

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    client  = bq_hook.get_client(project_id=BQ_PROJECT)

    job_config = LoadJobConfig(
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    destination = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_STAGING_TABLE}"
    job = client.load_table_from_dataframe(df, destination, job_config=job_config)
    job.result()

    log.info(f"STAGING: załadowano {len(df)} rekordów do {destination}")


# ---------------------------------------------------------------------------
# Krok 4b: MERGE STAGING → OSM_OBJECTS (jak merge_to_raw w bigquery.py)
# ---------------------------------------------------------------------------
def merge_osm_to_raw(**context):
    """
    MERGE z OSM_OBJECTS_STAGING do OSM_OBJECTS po kluczu osm_id.
    - istniejący osm_id → UPDATE (np. zmiana tagów w OSM)
    - nowy osm_id → INSERT

    Gdy tabela OSM_OBJECTS jeszcze nie istnieje, tworzy ją jako kopię STAGING
    (pierwszy run — analogicznie do pełnego załadowania RAW).
    """
    log = logging.getLogger(__name__)

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    client = bq_hook.get_client(project_id=BQ_PROJECT)
    target_fq = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    staging_fq = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_STAGING_TABLE}"

    try:
        client.get_table(target_fq)
    except NotFound:
        log.info(f"Tabela {BQ_TABLE} nie istnieje — CREATE TABLE AS SELECT z STAGING (pierwszy run)")
        client.query(f"""
            CREATE TABLE `{target_fq}` AS
            SELECT * FROM `{staging_fq}`
        """).result()
        return

    cols = OSM_OBJECT_COLUMNS
    target  = f"`{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
    staging = f"`{BQ_PROJECT}.{BQ_DATASET}.{BQ_STAGING_TABLE}`"

    update_cols = [c for c in cols if c != "osm_id"]
    update_set  = ",\n        ".join(
        [f"target.{c} = source.{c}" for c in update_cols]
    )
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"source.{c}" for c in cols])

    merge_sql = f"""
        MERGE {target} AS target
        USING {staging} AS source
        ON target.osm_id = source.osm_id

        WHEN MATCHED THEN
            UPDATE SET
                {update_set}

        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
    """

    log.info(f"Wykonuję MERGE {BQ_STAGING_TABLE} → {BQ_TABLE}")
    client.query(merge_sql).result()
    log.info("MERGE OSM_OBJECTS zakończony")


# ---------------------------------------------------------------------------
# Krok 5: zapisz timestamp Overpass do tabeli META
# ---------------------------------------------------------------------------
def save_sync_timestamp(**context):
    """
    Po pomyślnym MERGE (lub utworzeniu RAW z STAGING) zapisuje do BQ_META_TABLE znacznik czasu
    `osm_base_timestamp` zwrócony przez Overpass API.

    Wybieramy najnowszy timestamp spośród wszystkich zapytanych województw
    (mogą się minimalnie różnić, jeśli Overpass był pod obciążeniem).

    Przy następnym uruchomieniu DAG-a `get_last_sync_timestamp` odczyta
    właśnie tę wartość i użyje jej jako dolnej granicy filtru `newer`.
    """
    log = logging.getLogger(__name__)
    ti = context["ti"]

    # Zbierz timestampy ze wszystkich województw i wybierz najnowszy
    timestamps = []
    for voivodeship in VOIVODESHIPS.keys():
        ts = ti.xcom_pull(
            task_ids=f"fetch_{voivodeship}",
            key=f"osm_base_ts_{voivodeship}"
        )
        if ts:
            timestamps.append(ts)

    if not timestamps:
        log.warning("Brak timestampów z Overpass — META nie zostanie zaktualizowana.")
        return

    # Porównujemy jako stringi ISO-8601 (sortowanie leksykograficzne działa
    # poprawnie dla dat w tym formacie)
    latest_ts = max(timestamps)
    run_at    = datetime.now(timezone.utc).isoformat()
    log.info(f"Zapisuję osm_base_timestamp: {latest_ts}")

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    client  = bq_hook.get_client(project_id=BQ_PROJECT)

    meta_row = pd.DataFrame([{
        "osm_base_timestamp": latest_ts,
        "run_at":             run_at,
    }])

    job_config = LoadJobConfig(
        write_disposition=WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )

    destination = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_META_TABLE}"
    job = client.load_table_from_dataframe(meta_row, destination, job_config=job_config)
    job.result()

    log.info(f"Timestamp zapisany do {destination}")


# ---------------------------------------------------------------------------
# Krok 6 (pomocniczy): liczba wierszy w RAW.OSM_OBJECTS (do maila)
# ---------------------------------------------------------------------------
def _count_raw_osm_rows() -> int | None:
    """Zwraca COUNT(*) z RAW lub None, gdy tabela nie istnieje / błąd odczytu."""
    log = logging.getLogger(__name__)
    fqtn = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client = bq_hook.get_client(project_id=BQ_PROJECT)
        rows = list(client.query(f"SELECT COUNT(1) AS n FROM `{fqtn}`").result())
        return int(rows[0]["n"]) if rows else 0
    except NotFound:
        log.warning("Licznik RAW: brak tabeli %s", fqtn)
        return None
    except Exception as e:
        log.warning("Licznik RAW: %s", e)
        return None


# ---------------------------------------------------------------------------
# Krok 6 (pomocniczy): buduje treść HTML maila z danych XCom
# ---------------------------------------------------------------------------
def _build_email_html(**context) -> str:
    """
    Buduje treść HTML maila na podstawie statystyk z XCom.

    Treść różni się w zależności od trybu:
      - PEŁNY  (pierwszy run, brak META)  — informacja o pełnym snapshoCie
      - INKREMENTALNY                     — zakres dat i delta rekordów
    """
    ti = context["ti"]

    newer_since = ti.xcom_pull(task_ids="get_last_sync_timestamp", key="newer_since")
    total       = ti.xcom_pull(task_ids="check_new_data", key="staging_count") or 0
    counts      = ti.xcom_pull(task_ids="check_new_data", key="counts_per_voivodeship") or {}
    run_date    = context["ds"]   # data logiczna runu (YYYY-MM-DD)

    # Tryb: pełny (brak poprzedniego timestampa) albo inkrementalny
    if newer_since:
        mode_label  = "INKREMENTALNY"
        mode_detail = f"Zmiany od: <code>{newer_since}</code>"
    else:
        mode_label  = "PEŁNY (pierwszy run)"
        mode_detail = "Brak poprzedniego timestampa — pobrano pełny snapshot obszaru."

    # Wiersze tabeli per województwo
    rows_html = "".join(
        f"<tr>"
        f"<td style='padding:5px 14px;border:1px solid #ccc'>{voi}</td>"
        f"<td style='padding:5px 14px;border:1px solid #ccc;text-align:right'><b>{cnt}</b></td>"
        f"</tr>"
        for voi, cnt in counts.items()
    )

    raw_total = _count_raw_osm_rows()
    raw_cell = f"<b>{raw_total}</b> obiektów" if raw_total is not None else "—"

    return f"""
    <html><body style="font-family:sans-serif;color:#222;max-width:600px">
      <h2 style="color:#1a6e3c">&#x26EA; OSM Shrines — podsumowanie runu</h2>

      <table style="margin-bottom:12px">
        <tr><td style="color:#555;padding-right:12px">Data runu</td>
            <td><b>{run_date}</b></td></tr>
        <tr><td style="color:#555;padding-right:12px">Tryb</td>
            <td><b>{mode_label}</b></td></tr>
        <tr><td style="color:#555;padding-right:12px">Zakres</td>
            <td>{mode_detail}</td></tr>
        <tr><td style="color:#555;padding-right:12px">Łącznie w STAGING</td>
            <td><b>{total}</b> obiektów</td></tr>
        <tr><td style="color:#555;padding-right:12px">Łącznie w RAW</td>
            <td>{raw_cell}</td></tr>
      </table>

      <table style="border-collapse:collapse">
        <thead>
          <tr style="background:#e8f5e9">
            <th style="padding:5px 14px;border:1px solid #ccc;text-align:left">Województwo</th>
            <th style="padding:5px 14px;border:1px solid #ccc;text-align:right">Rekordów</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>

      <p style="margin-top:20px;color:#555;font-size:0.85em;border-top:1px solid #eee;padding-top:8px">
        Tabela docelowa: <code>{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}</code><br>
        Staging: <code>{BQ_PROJECT}.{BQ_DATASET}.{BQ_STAGING_TABLE}</code>
      </p>
    </body></html>
    """


def _send_osm_notify_email(**context) -> None:
    """Wysyła mail przez `send_email` (SMTP) — treść z XCom przez `_build_email_html`."""
    html = _build_email_html(**context)
    subject = f"[OSM Shrines] Podsumowanie — {context['ds']}"
    to = [NOTIFY_EMAIL] if isinstance(NOTIFY_EMAIL, str) else list(NOTIFY_EMAIL)
    send_email(to=to, subject=subject, html_content=html)


# ============================================================
# Definicja DAG-a
# ============================================================
with DAG(
    dag_id="fetch_osm_shrines_to_bigquery",
    start_date=datetime(2024, 1, 1),
    # Uruchamiany codziennie o 3:00 UTC (zamiast co tydzień),
    # co pozwala na naprawdę inkrementalne śledzenie zmian w OSM.
    schedule="0 3 * * *",
    catchup=False,
    tags=["osm", "bigquery"],
    doc_md="""
    ## OSM → BigQuery RAW (`OSM_OBJECTS` + STAGING + META)

    Jak `extract_mariadb_to_bigquery_raw` (GRAVES/CENSUS), ale źródłem jest Overpass API:

    1. Odczyt `osm_base_timestamp` z `RAW.OSM_OBJECTS_META` → filtr Overpass `newer`.
    2. Pobranie w bboxach województw (pełny snapshot przy pierwszym runie bez META).
    3. Partia z kroku 2 → `RAW.OSM_OBJECTS_STAGING` w BigQuery (`WRITE_TRUNCATE`, task `osm_to_staging`).
    4. `MERGE` STAGING → `RAW.OSM_OBJECTS` po `osm_id` (INSERT / UPDATE). Pierwszy run:
       brak docelowej tabeli → `CREATE TABLE … AS SELECT` ze STAGING.
    5. Zapis timestampu do `RAW.OSM_OBJECTS_META` (wyłącznie gdy partia niepusta — gałąź staging).
    6. Mail po `done` (join gałęzi z danymi / pustą partią). SMTP: `AIRFLOW__SMTP__*` lub `[smtp]` w airflow.cfg.

    Stare tabele `OSM_SHRINES` / `OSM_SHRINES_META` nie są używane — migracja danych
    w BigQuery, jeśli potrzebna, osobno (np. `INSERT … SELECT`).
    """,
) as dag:

    # ------------------------------------------------------------------
    # T0: odczytaj timestamp ostatniego synca z tabeli META
    # ------------------------------------------------------------------
    get_last_sync = PythonOperator(
        task_id="get_last_sync_timestamp",
        python_callable=get_last_sync_timestamp,
    )

    # ------------------------------------------------------------------
    # T1..N: pobierz dane z Overpass dla każdego województwa (sekwencyjnie)
    # Sekwencyjność jest celowa — chroni publiczną instancję Overpass
    # przed zbyt wieloma równoległymi zapytaniami z jednego źródła.
    # ------------------------------------------------------------------
    fetch_tasks = []
    for voivodeship, bbox in VOIVODESHIPS.items():
        task = PythonOperator(
            task_id=f"fetch_{voivodeship}",
            python_callable=fetch_overpass,
            op_kwargs={
                "voivodeship": voivodeship,
                "bbox": bbox,
            },
        )
        fetch_tasks.append(task)

    # Sekwencja fetch tasków
    for i in range(len(fetch_tasks) - 1):
        fetch_tasks[i] >> fetch_tasks[i + 1]

    # ------------------------------------------------------------------
    # T_check: czy partia z Overpass jest niepusta (branching)
    # ------------------------------------------------------------------
    check = BranchPythonOperator(
        task_id="check_new_data",
        python_callable=check_new_data,
    )

    # ------------------------------------------------------------------
    # T_staging: partia → OSM_OBJECTS_STAGING w BQ (WRITE_TRUNCATE)
    # ------------------------------------------------------------------
    osm_to_staging = PythonOperator(
        task_id="osm_to_staging",
        python_callable=load_osm_to_staging,
    )

    # ------------------------------------------------------------------
    # T_merge: MERGE STAGING → OSM_OBJECTS
    # ------------------------------------------------------------------
    merge_osm = PythonOperator(
        task_id="merge_osm_to_raw",
        python_callable=merge_osm_to_raw,
    )

    # ------------------------------------------------------------------
    # T_meta: zapisz timestamp do META (po MERGE)
    # ------------------------------------------------------------------
    save_meta = PythonOperator(
        task_id="save_sync_timestamp",
        python_callable=save_sync_timestamp,
    )

    # ------------------------------------------------------------------
    # T_email: zawsze po joinie `done` (Airflow 3 — PythonOperator + send_email).
    # ------------------------------------------------------------------
    notify = PythonOperator(
        task_id="notify_new_records",
        python_callable=_send_osm_notify_email,
    )

    # ------------------------------------------------------------------
    # T_skip: nic nie rób, gdy brak nowych danych
    # ------------------------------------------------------------------
    skip = EmptyOperator(task_id="skip")

    # ------------------------------------------------------------------
    # T_done: join obu gałęzi brancha (dokładnie jedna kończy się sukcesem)
    # ------------------------------------------------------------------
    done = EmptyOperator(
        task_id="done",
        trigger_rule="none_failed_min_one_success",
    )

    # ------------------------------------------------------------------
    # Graf zależności
    #
    #  get_last_sync
    #       │
    #  fetch_woj_1 >> fetch_woj_2 >> ... >> fetch_woj_N
    #                                              │
    #                                          check_new_data
    #                                         /              \
    #         osm_to_staging >> merge_osm >> save_meta       skip
    #                           \              |             |
    #                            \             v             v
    #                             `------------ done --------´
    #                                            |
    #                                    notify_new_records
    # ------------------------------------------------------------------
    get_last_sync >> fetch_tasks[0]
    fetch_tasks[-1] >> check
    check >> osm_to_staging >> merge_osm >> save_meta >> done
    check >> skip >> done
    done >> notify