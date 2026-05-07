from google.cloud import bigquery
import dlt
import google.auth
from google.api_core.exceptions import NotFound

@dlt.resource(name="pages", primary_key="page_id")
def get_pages(from_date, to_date):
    print(f"Fetching data from {from_date} to {to_date}")
    source_project_id = "aller-data-platform-prod-1f89"
    source_dataset = "editorial"
    source_table = "dim_pages"
    
    client = bigquery.Client()
    # Get existing page_id from target table to avoid duplicates
    existing_ids = get_existing_ids(client, from_date, to_date)

    credentials, project_id = google.auth.default()
    print(f"Project ID: {project_id}")

    source_query = f"""
        SELECT
            page_id,
            site_domain,
            market,
            cms_page_id,
            title,
            intro,
            bodytext,
            bodytext_html,
            tags,
            section,
            author,
            url,
            page_type,
            lock_status,
            published_local_dt,
            published_ts,
            updated_ts,
            created_ts,
            created_by,
            verticals
        FROM `{source_project_id}.{source_dataset}.{source_table}`
        WHERE published_ts BETWEEN TIMESTAMP('{from_date}') AND TIMESTAMP('{to_date}')
    """
    source_job = client.query(source_query)

    for row in source_job:
        if row.page_id not in existing_ids:
            yield dict(row)

def get_existing_ids(target_client, from_date, to_date):
    target_dataset = "adp_pages"
    target_table = "pages"

    existing_ids_query = f"""
        SELECT page_id FROM `{target_client.project}.{target_dataset}.{target_table}`
        WHERE published_ts BETWEEN TIMESTAMP('{from_date}') AND TIMESTAMP('{to_date}')
    """
    try:
        existing_ids_job = target_client.query(existing_ids_query)
        return {row.page_id for row in existing_ids_job}
    except NotFound:
        return set()
    