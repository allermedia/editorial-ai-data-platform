
from datetime import datetime, timezone, timedelta, time
import os
import json
from data_access import DataAccess
from platform_service import PlatformService

if __name__ == "__main__":
    project_id = os.environ.get('ADP_PROJECT_ID', 'aller-data-platform-prod-1f89')
    data_access = DataAccess(project_id)
    platform_service = PlatformService()

    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()

    pageviews = data_access.get_pageviews(
        event_date = yesterday
    )

    pages = data_access.get_cms_ids()

    site_metrics = {}
    for row in pageviews:
        domain = row.site_domain
        if domain not in site_metrics:
            site_metrics[domain] = {'total_sessions': 0, 'total_pageviews': 0, 'article_count': 0}
        site_metrics[domain]['total_sessions'] += row.sessions or 0
        site_metrics[domain]['total_pageviews'] += row.pageviews or 0
        site_metrics[domain]['article_count'] += 1

    site_averages = {}
    for domain, metrics in site_metrics.items():
        article_count = metrics['article_count']
        if article_count > 0:
            site_averages[domain] = {
                'sessionsSiteAverage': metrics['total_sessions'] / article_count,
                'pageviewsSiteAverage': metrics['total_pageviews'] / article_count
            }
        else:
            site_averages[domain] = {
                'sessionsSiteAverage': 0,
                'pageviewsSiteAverage': 0
            }

    final_rows = []
    for row in pageviews:
        cms_page_id = pages.get(row.page_id)
        if cms_page_id:
            event_date = row.event_date
            start_time = datetime.combine(event_date, time.min, tzinfo=timezone.utc)
            end_time = datetime.combine(event_date, time(23, 59, 59), tzinfo=timezone.utc)

            averages = site_averages.get(row.site_domain, {'sessionsSiteAverage': 0, 'pageviewsSiteAverage': 0})

            final_rows.append({
                "articleId": cms_page_id,
                "startTime": start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "endTime": end_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "metrics": {
                    "sessions": row.sessions,
                    "pageviews": row.pageviews,
                    "sessionsSiteAverage": averages['sessionsSiteAverage'],
                    "pageviewsSiteAverage": averages['pageviewsSiteAverage']
                }
            })

    with open('final_rows.json', 'w') as f:
        json.dump(final_rows, f, indent=4)
    
    platform_service.post_article_traffic_to_platform(
        data = final_rows,
        url = "https://article-gateway.ai.aller.com/api/v1/pageviews"
    )
