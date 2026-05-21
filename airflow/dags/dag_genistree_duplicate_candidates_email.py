from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime
from html import escape
import os

# ============================================================
# KONFIGURACJA
# ============================================================
GCP_CONN_ID = "google_cloud"
BQ_PROJECT = "genistry-379120"
BQ_DATASET = (os.getenv("GENISTREE_DUPLICATES_BQ_DATASET") or "dbt_MARTS").strip()
BQ_TABLE = "mart_genistree_shrines_crosses_duplicate_candidates"

DBT_MODEL = "mart_genistree_shrines_crosses_duplicate_candidates"

# Adresat powiadomien e-mail wysylanych po odswiezeniu raportu dubli.
# Ustaw w airflow/.env: GENISTREE_DUPLICATES_NOTIFY_EMAIL=...
NOTIFY_EMAIL = (
    os.getenv("GENISTREE_DUPLICATES_NOTIFY_EMAIL")
    or os.getenv("OSM_SHRINES_NOTIFY_EMAIL")
    or ""
).strip() or "twoj@gmail.com"
# ============================================================


def _format_cell(value) -> str:
    if value is None:
        return ""
    return escape(str(value))


def _query_duplicate_candidates() -> list[dict]:
    """Pobiera wszystkie pary kandydatow na duble z gotowego marta BigQuery."""
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    client = bq_hook.get_client(project_id=BQ_PROJECT)

    query = f"""
        select
            genistree_uid_a,
            custom_document_type_id_a,
            address_a,
            place_a,
            year_a,
            genistree_uid_b,
            custom_document_type_id_b,
            address_b,
            place_b,
            year_b,
            distance_m
        from `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
        order by distance_m, genistree_uid_a, genistree_uid_b
    """
    return [dict(row.items()) for row in client.query(query).result()]


def _build_email_html(rows: list[dict], run_date: str) -> str:
    total = len(rows)

    if rows:
        rows_html = "".join(
            "<tr>"
            f"<td style='padding:5px 8px;border:1px solid #ccc'>{_format_cell(row['genistree_uid_a'])}</td>"
            f"<td style='padding:5px 8px;border:1px solid #ccc'>{_format_cell(row['genistree_uid_b'])}</td>"
            f"<td style='padding:5px 8px;border:1px solid #ccc;text-align:right'>{_format_cell(row['custom_document_type_id_a'])}</td>"
            f"<td style='padding:5px 8px;border:1px solid #ccc;text-align:right'>{_format_cell(row['custom_document_type_id_b'])}</td>"
            f"<td style='padding:5px 8px;border:1px solid #ccc'>{_format_cell(row['address_a'])}</td>"
            f"<td style='padding:5px 8px;border:1px solid #ccc'>{_format_cell(row['address_b'])}</td>"
            f"<td style='padding:5px 8px;border:1px solid #ccc'>{_format_cell(row['place_a'])}</td>"
            f"<td style='padding:5px 8px;border:1px solid #ccc'>{_format_cell(row['place_b'])}</td>"
            f"<td style='padding:5px 8px;border:1px solid #ccc'>{_format_cell(row['year_a'])}</td>"
            f"<td style='padding:5px 8px;border:1px solid #ccc'>{_format_cell(row['year_b'])}</td>"
            f"<td style='padding:5px 8px;border:1px solid #ccc;text-align:right'><b>{_format_cell(row['distance_m'])}</b></td>"
            "</tr>"
            for row in rows
        )
    else:
        rows_html = (
            "<tr>"
            "<td colspan='11' style='padding:8px 12px;border:1px solid #ccc;color:#555'>"
            "Brak potencjalnych dubli wedlug aktualnego progu odleglosci."
            "</td>"
            "</tr>"
        )

    return f"""
    <html><body style="font-family:sans-serif;color:#222;max-width:1200px">
      <h2 style="color:#7a4b00">Genistree - potencjalne duble kapliczek i krzyzy</h2>

      <table style="margin-bottom:12px">
        <tr><td style="color:#555;padding-right:12px">Data runu</td>
            <td><b>{escape(run_date)}</b></td></tr>
        <tr><td style="color:#555;padding-right:12px">Liczba par</td>
            <td><b>{total}</b></td></tr>
        <tr><td style="color:#555;padding-right:12px">Tabela zrodlowa</td>
            <td><code>{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}</code></td></tr>
      </table>

      <table style="border-collapse:collapse;font-size:0.9em">
        <thead>
          <tr style="background:#fff3cd">
            <th style="padding:5px 8px;border:1px solid #ccc;text-align:left">id A</th>
            <th style="padding:5px 8px;border:1px solid #ccc;text-align:left">id B</th>
            <th style="padding:5px 8px;border:1px solid #ccc;text-align:right">typ A</th>
            <th style="padding:5px 8px;border:1px solid #ccc;text-align:right">typ B</th>
            <th style="padding:5px 8px;border:1px solid #ccc;text-align:left">address A</th>
            <th style="padding:5px 8px;border:1px solid #ccc;text-align:left">address B</th>
            <th style="padding:5px 8px;border:1px solid #ccc;text-align:left">place A</th>
            <th style="padding:5px 8px;border:1px solid #ccc;text-align:left">place B</th>
            <th style="padding:5px 8px;border:1px solid #ccc;text-align:left">year A</th>
            <th style="padding:5px 8px;border:1px solid #ccc;text-align:left">year B</th>
            <th style="padding:5px 8px;border:1px solid #ccc;text-align:right">distance_m</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>

      <p style="margin-top:20px;color:#555;font-size:0.85em;border-top:1px solid #eee;padding-top:8px">
        Raport bazuje na modelu dbt <code>{DBT_MODEL}</code>. Pary sa wyznaczane tylko
        w obrebie tego samego <code>custom_document_type_id</code>.
      </p>
    </body></html>
    """


def send_duplicate_candidates_email(**context) -> None:
    rows = _query_duplicate_candidates()
    html = _build_email_html(rows=rows, run_date=context["ds"])
    subject = f"[Genistree] Potencjalne duble kapliczek/krzyzy - {context['ds']}"
    to = [NOTIFY_EMAIL] if isinstance(NOTIFY_EMAIL, str) else list(NOTIFY_EMAIL)
    send_email(to=to, subject=subject, html_content=html)


# ============================================================
# Definicja DAG-a
# ============================================================
with DAG(
    dag_id="dag_genistree_duplicate_candidates_email",
    start_date=datetime(2024, 1, 1),
    schedule="0 5 * * *",
    catchup=False,
    tags=["genistree", "dbt", "report", "email"],
    doc_md="""
    ## Raport mailowy: potencjalne duble kapliczek i krzyzy Genistree

    1. Czyta wynik z `dbt_MARTS.mart_genistree_shrines_crosses_duplicate_candidates`.
    2. Wysyla mail z tabela wszystkich par potencjalnych dubli.

    Model dbt musi byc odswiezany osobno przed uruchomieniem raportu.
    Dataset BigQuery mozna nadpisac przez `GENISTREE_DUPLICATES_BQ_DATASET`.
    """,
) as dag:

    notify = PythonOperator(
        task_id="send_duplicate_candidates_email",
        python_callable=send_duplicate_candidates_email,
    )
