from tecton import sql_transformation, TemporalFeaturePackage, MaterializationConfig
from feature_repo.shared import entities as e, data_sources
from datetime import datetime


@sql_transformation(inputs=data_sources.ad_impressions_batch, has_context=True)
def ad_ground_truth_ctr_performance_7_days_transformer(context, ad_impressions_batch):
    return f"""
    SELECT
        ad_id,
        sum(clicked) as ad_total_clicks_7days,
        count(*) as ad_total_impressions_7days,
        to_timestamp('{context.feature_data_end_time}') as timestamp
    FROM
        {ad_impressions_batch}
    GROUP BY
        ad_id
    """


ad_ground_truth_ctr_performance_7_days = TemporalFeaturePackage(
    name="ad_ground_truth_ctr_performance_7_days",
    description="[SQL Feature] The aggregate CTR of a partner website (clicks / total impressions) over the last 7 days",
    transformation=ad_ground_truth_ctr_performance_7_days_transformer,
    entities=[e.ad_entity],
    materialization=MaterializationConfig(
        online_enabled=True,
        offline_enabled=True,
        feature_start_time=datetime(2020, 6, 19),
        schedule_interval="1day",
        serving_ttl="1day",
        data_lookback_period="7days",
    ),
    family="ad_serving",
    tags={"release": "production"},
    owner="matt@tecton.ai",
)
