from airflow.providers.postgres.hooks.postgres import PostgresHook

def _fetch_top_company(ti):
    """
    Fetches the company with the highest total view counts from the database.
    """
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    query = """
        SELECT company, SUM(view_count) AS number_of_views
        FROM wikipedia_pageviews
        GROUP BY company
        ORDER BY number_of_views DESC
        LIMIT 1;
    """
    records = postgres_hook.get_records(query)[0]
    ti.xcom_push(key="top_company", value=records)