
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

    final_rows = []
    for row in pageviews:
        cms_page_id = pages.get(row.page_id)
        if cms_page_id:
            event_date = row.event_date
            start_time = datetime.combine(event_date, time.min, tzinfo=timezone.utc)
            end_time = datetime.combine(event_date, time(23, 59, 59), tzinfo=timezone.utc)

            final_rows.append({
                "articleId": cms_page_id,
                "startTime": start_time.isoformat().replace('+00:00', 'Z'),
                "endTime": end_time.isoformat().replace('+00:00', 'Z'),
                "metrics": {
                    "sessions": row.sessions,
                    "pageviews": row.pageviews
                }
            })

    with open('final_rows.json', 'w') as f:
        json.dump(final_rows, f, indent=4)
    
    platform_service.post_article_traffic_to_platform(
        data = final_rows,
        url = "https://article-gateway.ai.aller.com/api/v1/pageviews"
    )
