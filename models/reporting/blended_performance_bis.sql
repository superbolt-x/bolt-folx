{{ config (
    alias = target.database + '_blended_performance_bis'
)}}
{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

-- Create a CTE for adjusting dates for Sunday-based weeks
WITH initial_memb_data as (
    SELECT *, {{ get_date_parts('date') }}
    FROM {{ source('gsheet_raw', 'memberships') }}
),
initial_fb_data as (
    SELECT *, {{ get_date_parts('date') }}
    FROM {{ source('facebook_raw', 'campaigns_insights_region') }}
),
initial_reddit_data as (
    SELECT *, {{ get_date_parts('date') }}
    FROM {{ source('reddit_raw', 'campaign_region_insights') }}
),
initial_google_data as (
    SELECT *, {{ get_date_parts('date') }}
    FROM {{ source('googleads_raw', 'account_region_report') }}
),
initial_tiktok_data as (
    SELECT *, {{ get_date_parts('date') }}
    FROM {{ source('supermetrics_raw', 'tik_campaign_insights_region') }}
),
reddit_campaigns as (
    SELECT campaign_id, campaign_name
    FROM {{ source('reddit_base', 'reddit_campaigns') }}
),

SELECT 
    channel, 
    campaign_name::varchar as campaign_name, 
    date, 
    date_granularity,
    us_state,
    state_category,
    COALESCE(SUM(spend), 0) as spend, 
    COALESCE(SUM(impressions), 0) as impressions, 
    COALESCE(SUM(clicks), 0) as clicks,
    COALESCE(SUM(trials), 0) as trials, 
    COALESCE(SUM(memberships), 0) as memberships
FROM
    ({% for date_granularity in date_granularity_list %}
    SELECT
        'Facebook' as channel, 
        campaign_name::varchar as campaign_name, 
        {{date_granularity}} as date, 
        '{{date_granularity}}' as date_granularity,
        {{ state_name_to_code('region') }} as us_state,
        spend, 
        impressions, 
        inline_link_clicks as clicks, 
        0 as trials, 
        0 as memberships
    FROM initial_fb_data fp
    
    UNION ALL
    
    SELECT 
        'Google' as channel, 
        campaign_name::varchar as campaign_name,
         {{date_granularity}} as date,
        '{{date_granularity}}' as date_granularity,
        {{ state_name_to_code('name') }} as us_state,
        cost_micros::float/1000000 as spend,
        impressions,
        clicks,
        0 as trials,
        0 as memberships
    FROM initial_google_data gp
    LEFT JOIN {{ source('googleads_raw', 'geo_target') }} g ON REPLACE(gp.geo_target_state, 'geoTargetConstants/', '') = g.id
    
    UNION ALL
    
    SELECT 
        'TikTok' as channel, 
        campaign_name::varchar as campaign_name,
        {{date_granularity}} as date,
        '{{date_granularity}}' as date_granularity,
        {{ state_name_to_code('province_name') }} as us_state,
        cost as spend,
        impressions,
        clicks,
        0 as trials,
        0 as memberships
    FROM initial_tiktok_data tp
    
    UNION ALL
    
    SELECT 
        'Reddit' as channel, 
        rc.campaign_name as campaign_name,
        {{date_granularity}} as date,
        '{{date_granularity}}' as date_granularity,
        g.region as us_state,
        spend::float/1000000,
        impressions,
        clicks,
        0 as trials,
        0 as memberships
    FROM initial_reddit_data rp
    LEFT JOIN {{ source('reddit_raw', 'geolocation') }} g ON rp.metro = g.dma AND g.country = 'US' AND g.dma != 0
    LEFT JOIN reddit_campaigns rc ON rp.campaign_id = rc.campaign_id
    
    UNION ALL
    
    SELECT 
        'Memberships' as channel,
        NULL::varchar as campaign_name,
        {{date_granularity}} as date,
        '{{date_granularity}}' as date_granularity,
        region::varchar as us_state,
        0::integer as spend, 
        0::integer as impressions, 
        0::integer as clicks, 
        trials, 
        memberships
    FROM initial_memb_data m
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %})
LEFT JOIN (SELECT category as state_category, states::varchar as us_state FROM {{ source('gsheet_raw', 'state_category') }}) USING (us_state)
GROUP BY channel, campaign_name, date, date_granularity, us_state, state_category
