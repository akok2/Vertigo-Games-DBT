select
    event_date,
    coalesce(country, 'UNKNOWN') as country,
    coalesce(platform, 'UNKNOWN') as platform,

    count(distinct user_id) as dau,

    sum(iap_revenue) as total_iap_revenue,
    sum(ad_revenue) as total_ad_revenue,

    safe_divide(sum(iap_revenue + ad_revenue), count(distinct user_id)) as arpdau,

    sum(match_start_count) as matches_started,
    safe_divide(sum(match_start_count), count(distinct user_id)) as match_per_dau,

    safe_divide(sum(victory_count), nullif(sum(match_end_count), 0)) as win_ratio,
    safe_divide(sum(defeat_count), nullif(sum(match_end_count), 0)) as defeat_ratio,

    safe_divide(sum(server_connection_error), count(distinct user_id)) as server_error_per_dau

from {{ source('raw', 'user_daily_metrics') }}

group by 1,2,3

