{{ config (
    alias = target.database + '_folx_blended_table'
)}}

-- Facebook data
(SELECT 
    'Facebook' as channel,
    campaign_name,
    date,
    date_granularity,
    COALESCE(SUM(spend), 0) as spend,
    COALESCE(SUM(impressions), 0) as impressions,
    COALESCE(SUM(link_clicks), 0) as clicks,
    0 as trials,
    0 as memberships
FROM {{ source('reporting', 'folx_facebook_campaign_performance') }}
GROUP BY channel, campaign_name, date, date_granularity)

UNION ALL

-- Google Adwords data
(SELECT 
    'Google' as channel,
    campaign_name,
    date,
    date_granularity,
    COALESCE(SUM(spend), 0) as spend,
    COALESCE(SUM(impressions), 0) as impressions,
    COALESCE(SUM(clicks), 0) as clicks,
    0 as trials,
    0 as memberships
FROM {{ source('reporting', 'folx_googleads_campaign_performance') }}
GROUP BY channel, campaign_name, date, date_granularity)

UNION ALL

-- TikTok data
(SELECT 
    'TikTok' as channel,
    campaign_name,
    date,
    date_granularity,
    COALESCE(SUM(spend), 0) as spend,
    COALESCE(SUM(impressions), 0) as impressions,
    COALESCE(SUM(clicks), 0) as clicks,
    0 as trials,
    0 as memberships
FROM {{ source('reporting', 'folx_tiktok_ads_performance') }}
GROUP BY channel, campaign_name, date, date_granularity)

UNION ALL

-- Reddit data
(SELECT 
    'Reddit' as channel,
    campaign_name,
    date,
    date_granularity,
    COALESCE(SUM(spend), 0) as spend,
    COALESCE(SUM(impressions), 0) as impressions,
    COALESCE(SUM(clicks), 0) as clicks,
    0 as trials,
    0 as memberships
FROM {{ source('reporting', 'folx_reddit_performance_by_ad') }}
GROUP BY channel, campaign_name, date, date_granularity)

UNION ALL

-- Trials/Membership data
(SELECT 
    'Organic' as channel,
    NULL as campaign_name,
    date,
    'day' as date_granularity,
    0 as spend,
    0 as impressions,
    0 as clicks,
    COALESCE(SUM(trials), 0) as trials,
    COALESCE(SUM(memberships), 0) as memberships
FROM {{ source('gsheet_raw', 'memberships') }}
GROUP BY channel, campaign_name, date, date_granularity)

UNION ALL

-- Trials/Membership data with attributed spend from Meta
(SELECT 
    'Facebook' as channel,
    NULL as campaign_name,
    date,
    'day' as date_granularity,
    COALESCE(SUM(meta_spend), 0) as spend,
    0 as impressions,
    0 as clicks,
    COALESCE(SUM(trials), 0) as trials,
    COALESCE(SUM(memberships), 0) as memberships
FROM {{ source('gsheet_raw', 'memberships') }}
WHERE meta_spend IS NOT NULL AND meta_spend > 0
GROUP BY channel, campaign_name, date, date_granularity)

UNION ALL

-- Trials/Membership data with attributed spend from Google Adwords
(SELECT 
    'Google' as channel,
    NULL as campaign_name,
    date,
    'day' as date_granularity,
    COALESCE(SUM(adw_spend), 0) as spend,
    0 as impressions,
    0 as clicks,
    COALESCE(SUM(trials), 0) as trials,
    COALESCE(SUM(memberships), 0) as memberships
FROM {{ source('gsheet_raw', 'memberships') }}
WHERE adw_spend IS NOT NULL AND adw_spend > 0
GROUP BY channel, campaign_name, date, date_granularity)
