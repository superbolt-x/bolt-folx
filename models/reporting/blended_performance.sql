{{ config (
    alias = target.database + '_blended_performance'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH initial_memb_data as
    (SELECT *, {{ get_date_parts('date') }}
    FROM {{ source('gsheet_raw', 'memberships') }}
    )

SELECT channel, campaign_name, date, date_granularity, COALESCE(SUM(spend), 0) as spend, COALESCE(SUM(impressions), 0) as impressions, COALESCE(SUM(clicks), 0) as clicks,
    COALESCE(SUM(trials), 0) as trials, COALESCE(SUM(memberships), 0) as memberships
FROM
        ({% for date_granularity in date_granularity_list %}
        SELECT 'Memberships' as channel, NULL::varchar as campaign_name, {{date_granularity}} as date, '{{date_granularity}}' as date_granularity, 
            0::integer as spend, 0::integer as impressions, 0::integer as clicks, trials, memberships
        FROM initial_memb_data
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %})
GROUP BY channel, campaign_name, date, date_granularity
