import re
from airflow.providers.postgres.hooks.postgres import PostgresHook

def _get_page_names(file_path: str) -> list[dict]:
    """
    Extracts page names and view counts for specific companies from the given file.
    """
    companies = ["Amazon", "Microsoft", "Google", "Apple", "Facebook"]
    extracted_pages = []
    with open(file_path, "r") as file:
        for line in file:
            page_fields = line.strip().split()
            for company in companies:
                pattern = fr"^{company}(?:[._']|$)"
                if re.match(pattern, page_fields[1]):
                    extracted_pages.append({
                    "company": company,
                    "page_title": page_fields[1],
                    "no_views": int(page_fields[2])
                })
                    break
    return extracted_pages

def load_data_postgres(file_path: str):
    """
    Loads extracted page names and view counts into the Postgres database.
    """
    pages = _get_page_names(file_path)
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    insert_query = """
        INSERT INTO wikipedia_pageviews (company, page_name, view_count)
        VALUES (%s, %s, %s)
    """
    for page in pages:
        postgres_hook.run(
            insert_query,
            parameters=(page["company"], page["page_title"], page["no_views"])
        )
