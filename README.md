# genistree_etl

Repozytorium **ETL** dla projektu **Genistree** — potoku danych od aplikacji i zewnętrznych źródeł do analityki na **Google BigQuery**.

- **Airflow** — DAG-i orkiestrujące zasilanie warstwy **RAW** w BigQuery: m.in. inkrementalny odczyt z **MariaDB** (Appwrite) przez tunel SSH dla kolekcji Genistree (`dag_genistree_import`) oraz pobieranie i scalanie obiektów **OpenStreetMap** związanych z kapliczkami i krzyżami przydrożnymi (`dag_osm_shrines_import`) - raport z podsumowaniem importu wysyłany mailowo. Osobny DAG `dag_genistree_duplicate_candidates_email` wysyła raport mailowy z potencjalnymi dublami kapliczek i krzyży na podstawie gotowego marta dbt.
- **dbt** — modele **staging** i **marts** nad datasetem RAW: czyszczenie, typowanie i zestawienia (np. mapy, statystyki), spójne z konwencją źródeł zdefiniowanych w `sources.yml`.

## Raporty (marts)

W BigQuery (dataset dbt **`dbt_MARTS`**) są tabele pod typowe pytania biznesowe i kontrolę jakości danych:

- **Aktywność i udział typów treści** — ilość rekordów zarejestrowanych w Genistree per user pogrupowane wg typu (pivot): **`mart_user_record_stats_long`**.
- **Mapa obiektów architektury sakralnej** — jedna warstwa mapy łączy obiekty (np. kapliczki, krzyże przydrożne) z **Genistree** i z **OpenStreetMap**, żeby widzieć **braki po stronie Genistree** (co jest w OSM, a nie ma u nas) oraz **braki po stronie OpenStreetMap**  i planować uzupełnianie którejkolwiek bazy: **`mart_map_shrines_crosses_osm_genistree`**.
- **Potencjalne duble kapliczek i krzyży Genistree** — pary rekordów z Genistree (`CustomDocumentTypeID` 1–5) tego samego typu, oddalone od siebie maksymalnie o 50 metrów: **`mart_genistree_shrines_crosses_duplicate_candidates`**.
- **Demografia zgonów w czasie** — jak w kolejnych dekadach rozkłada się liczba osób z podanym wiekiem, w podziale na grupy wiekowe (dzieci, dorośli, seniorzy): **`mart_person_death_timeline_counts`**.

## Cel

Utrzymanie jednego spójnego miejsca na surowe i przetworzone dane genealogiczno‑krajoznawcze (dokumenty, spisy, obiekty na mapie), tak aby raporty i kolejne narzędzia czytały już przygotowane widoki zamiast bezpośrednio z produkcyjnej bazy aplikacji.

