from google.cloud import bigquery
import json
from datetime import date, datetime, time, timezone, timedelta
import google.auth

class DataAccess:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.dataset = "editorial"
        

        quota_project = "aller-data-platform-prod-1f89"
        
        print(self.project_id)
        print(quota_project)
        self.client = bigquery.Client(project=quota_project)
        print("BigQuery client created successfully.")
        print("--- ADC DEBUGGING END ---")
            
    def get_pageviews(self, event_date):  
        table = "agg_web_traffic_by_page"

        print(f"Fetching pageviews for event_date: {event_date} from table: {table}")

        source_query = f"""
            SELECT
                event_date,
                page_id,
                market,
                site_domain,
                sessions,
                pageviews
            FROM 
                `{self.project_id}.{self.dataset}.{table}`
            WHERE 
                event_date = '{event_date}'
            AND
                page_id IS NOT NULL
        """
        job = self.client.query(source_query)
        
        rows = list(job.result())

        #transformed_data = [self.transform_row(dict(row)) for row in rows]

        return rows

    def get_cms_ids(self):
        table = "pages"

        query = f"""
            SELECT
                page_id,
                cms_page_id
            FROM 
                `{self.project_id}.{self.dataset}.{table}`
        """
        job = self.client.query(query)
        
        rows = list(job.result())
        page_rows = {row.page_id: row.cms_page_id for row in rows}

        return page_rows
