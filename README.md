# genistree_etl

Repozytorium **ETL** dla projektu **Genistree** — potoku danych od aplikacji i zewnętrznych źródeł do analityki na **Google BigQuery**.

## Co tu jest

- **Airflow** — DAG-i orkiestrujące zasilanie warstwy **RAW** w BigQuery: m.in. inkrementalny odczyt z **MariaDB** (Appwrite) przez tunel SSH dla kolekcji Genistree (`dag_genistree`) oraz pobieranie i scalanie obiektów **OpenStreetMap** związanych z kapliczkami i krzyżami przydrożnymi (`dag_osm_shrines`).
- **dbt** — modele **staging** i **marts** nad datasetem RAW: czyszczenie, typowanie i zestawienia (np. mapy, statystyki), spójne z konwencją źródeł zdefiniowanych w `sources.yml`.

## Cel

Utrzymanie jednego spójnego miejsca na surowe i przetworzone dane genealogiczno‑krajoznawcze (dokumenty, spisy, obiekty na mapie), tak aby raporty i kolejne narzędzia czytały już przygotowane widoki zamiast bezpośrednio z produkcyjnej bazy aplikacji.

## Konwencja nazw DAG-ów

Plik w `airflow/dags/` ma postać **`dag_<krótki_opis>.py`**, a **`dag_id` w kodzie = ta sama nazwa bez rozszerzenia** (np. `dag_genistree`, `dag_osm_shrines`). Dzięki temu lista w UI Airflow, ścieżki w logach i repozytorium łatwo się mapują; szczegóły przepływu są w `doc_md` i tagach (`genistree`, `raw`, domena typu `osm`), a nie w długim identyfikatorze DAG-a.
