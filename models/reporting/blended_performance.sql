{{ config (
    alias = target.database + '_blended_performance'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

SELECT channel, campaign_name, date, date_granularity, COALESCE(SUM(spend), 0) as spend, COALESCE(SUM(impressions), 0) as impressions, COALESCE(SUM(clicks), 0) as clicks,
    COALESCE(SUM(trials), 0) as trials, COALESCE(SUM(memberships), 0) as memberships
FROM
        ({% for date_granularity in date_granularity_list %}
        SELECT 
            'Facebook' as channel, campaign_name, date::date as date, date_granularity, 
                spend, impressions, link_clicks as clicks, 0 as trials, 0 as memberships
        FROM {{ source('reporting', 'facebook_campaign_performance') }}
        UNION ALL
        SELECT 
            'Google' as channel, campaign_name, date::date as date, date_granularity, 
                spend, impressions, clicks, 0 as trials, 0 as memberships
        FROM {{ source('reporting', 'googleads_campaign_performance') }}        
        UNION ALL
        SELECT 
            'TikTok' as channel, campaign_name, date::date as date, date_granularity, 
                spend, impressions, clicks, 0 as trials, 0 as memberships
        FROM {{ source('reporting', 'tiktok_ad_performance') }}
        UNION ALL
        SELECT 
            'Reddit' as channel, campaign_name, date::date as date, date_granularity, 
                spend, impressions, clicks, 0 as trials, 0 as memberships
        FROM {{ source('reporting', 'reddit_performance_by_ad') }}
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %})
GROUP BY channel, campaign_name, date, date_granularity
