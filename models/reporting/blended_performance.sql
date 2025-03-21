{{ config (
    alias = target.database + '_blended_performance'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

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
FROM {{ source('reporting', 'facebook_campaign_performance') }}
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
FROM {{ source('reporting', 'googleads_campaign_performance') }}
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
FROM {{ source('reporting', 'tiktok_ad_performance') }}
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
FROM {{ source('reporting', 'reddit_performance_by_ad') }}
GROUP BY channel, campaign_name, date, date_granularity)

UNION ALL

-- Trials/Membership data with all date granularities
{% for date_granularity in date_granularity_list %}
(SELECT 
    'Memberships' as channel,
    NULL as campaign_name,
    date_trunc('{{ date_granularity }}', date)::date AS date,
    '{{ date_granularity }}' as date_granularity,
    0 as spend,
    0 as impressions,
    0 as clicks,
    COALESCE(SUM(trials), 0) as trials,
    COALESCE(SUM(memberships), 0) as memberships
FROM {{ source('gsheet_raw', 'memberships') }}
GROUP BY 1,2,3,4)
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
