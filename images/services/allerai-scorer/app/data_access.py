import time
import pandas as pd
from google.cloud import bigquery
import google.auth
import traceback
import numpy as np

class DataManager:
    def __init__(self, adp_project_id, refresh_interval_seconds: int = 3600):
        self.refresh_interval = refresh_interval_seconds
        credentials, self.project_id = google.auth.default()
        self.client = bigquery.Client()
        self.adp_project_id = adp_project_id
        self._cached_articles: pd.DataFrame | None = None
        self._cached_tag_scores: pd.DataFrame | None = None
        self._cached_traffic_data: pd.DataFrame | None = None
        self._last_refresh: float = 0

    def _fetch_articles(self) -> pd.DataFrame:
        sql = f"""
        WITH ranked_articles AS (
            SELECT 
                page_id as article_id, 
                site_domain, 
                main_category,
                category,
                sub_category,
                text_embeddings_en as embeddings_en,
                ROW_NUMBER() OVER (PARTITION BY site_domain ORDER BY published_ts DESC) AS rn
            FROM `{self.adp_project_id}.editorial.dim_pages`
            WHERE page_type = 'Article'
            AND text_embeddings_en IS NOT NULL
        )
        SELECT 
            article_id, 
            site_domain, 
            main_category, 
            category, 
            sub_category, 
            embeddings_en
        FROM ranked_articles
        WHERE rn <= 1000
        """
        query_job = self.client.query(sql)
        df = query_job.result().to_dataframe()
        df = self.validate_embeddings_column(df)
        return df

    def _fetch_tag_scores(self) -> pd.DataFrame:
        sql = f"""
        SELECT 
            site,
            tag,
            frequency,
            total_articles,
            max_frequency,
            tag_type
            FROM `{self.project_id}.nordic_tag_scores.tag_scores`
        WHERE TRUE
        """
        query_job = self.client.query(sql)
        df = query_job.result().to_dataframe()
        return df

    def _fetch_traffic_data(self) -> pd.DataFrame:
        sql = f"""
        SELECT
            pv.page_id as article_id,
            pv.site_domain,
            SUM(pageview_count) AS pageviews_first_7_days
        FROM `{self.project_id}.adp_pageviews.pages_pageviews` pv
        JOIN (
            SELECT page_id, published_ts
            FROM `{self.project_id}.adp_pages.pages`
        ) p
        ON 
            pv.page_id = p.page_id
        WHERE 
            DATE(pv.event_date) BETWEEN DATE(p.published_ts) AND DATE_ADD(DATE(p.published_ts), INTERVAL 6 DAY)
        GROUP BY 
            pv.page_id, pv.site_domain
        """
        query_job = self.client.query(sql)
        df = query_job.result().to_dataframe()
        return df

    def refresh_cache(self) -> None:
        try:
            self._cached_articles = self._fetch_articles()
            self._cached_tag_scores = self._fetch_tag_scores()
            self._cached_traffic_data = self._fetch_traffic_data()
            self._last_refresh = time.time()
        except Exception:
            traceback.print_exc()
            raise

    def get_dataframes(self) -> dict[str, pd.DataFrame | None]:
        try:
            now = time.time()
            if (self._cached_articles is None or self._cached_tag_scores is None or self._cached_traffic_data is None
                    or (now - self._last_refresh) > self.refresh_interval):
                self.refresh_cache()
            return {
                "articles": self._cached_articles.copy() if self._cached_articles is not None else None,
                "tag_scores": self._cached_tag_scores.copy() if self._cached_tag_scores is not None else None,
                "traffic": self._cached_traffic_data.copy() if self._cached_traffic_data is not None else None
            }
        except Exception:
            traceback.print_exc()
            raise

    def validate_embeddings_column(self, df: pd.DataFrame) -> pd.DataFrame:
        def safe_pass(x):
            if isinstance(x, np.ndarray) and x.dtype in [np.float32, np.float64] and x.size > 0:
                return x
            return None

        df["embeddings_en"] = df["embeddings_en"].apply(safe_pass)
        return df
