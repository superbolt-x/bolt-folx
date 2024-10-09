{{ config (
    alias = target.database + '_googleads_ad_performance'
)}}

SELECT
account_id,
ad_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
CASE WHEN campaign_name IN 
    ('[CALIFORNIA] HRT-Estrogen_|_ch:googlesearch_|_ob:Nonbrand_|_gl:impressions_|_tt:keywords_|_dv:all_|_ge:states_|_c1:_|_c2:_|_c3:_|_jump',
    '[Superbolt] HRT Estrogen - NB Search - Florida',
    '[Superbolt] HRT Estrogen - NB Search - New York',
    '[Superbolt] HRT Estrogen - NB Search - Texas',
    '[Superbolt] HRT Estrogen - NB Search - Online Estrogen - All States',
    '[Superbolt] HRT Estrogen - NB Search - Remaining States') THEN 'NB Estrogen'
    ELSE 'Other'
END as campaign_type_custom,
ad_group_name,
ad_group_id,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue

FROM {{ ref('googleads_performance_by_ad') }}
