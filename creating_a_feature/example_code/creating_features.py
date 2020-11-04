

## 1. Register Data Sources and Entities
## Note: Data Sources and Entities are usually stored in their own files in a Feature Store Configuration.
from tecton import (
    VirtualDataSource,
    HiveDSConfig
)
from tecton import Entity

ad_impressions_hive = HiveDSConfig(
    database='ad_impressions_2',
    table='batch_events',
    timestamp_column_name='timestamp',
    date_partition_column='datestr'
)

ad_impressions_batch = VirtualDataSource(
    name="ad_impressions_batch",
    batch_ds_config=ad_impressions_hive,
    family='ad_serving',
    tags={
        'release': 'production',
        'source': 'mobile'
    }
)

partner_entity = Entity(name="PartnerWebsite", default_join_keys=["partner_id"],
                        description="The partner website participating in the ad network.")


## 2. Create a Feature Package
## Note: Each Feature Package often has its own file in a Feature Store Configuration.

from tecton import sql_transformation, TemporalFeaturePackage, MaterializationConfig
from datetime import datetime

@sql_transformation(inputs=ad_impressions_batch, has_context=True)
def partner_ctr_performance_transformer(context, ad_impressions_batch):
    return f"""
    SELECT
        partner_id,
        sum(clicked) / count(*) as partner_total_ctr,
        to_timestamp('{context.feature_data_end_time}') as timestamp
    FROM
        {ad_impressions_batch}
    GROUP BY
        partner_id
    """

partner_ctr_performance_7d = TemporalFeaturePackage(
    name="partner_ctr_performance",
    description="[SQL Feature] The aggregate CTR of a partner website (clicks / total impressions) over the past 7 days",
    transformation=partner_ctr_performance_transformer,
    entities=[partner_entity],
    materialization=MaterializationConfig(
        offline_enabled=True,
        online_enabled=False,
        feature_start_time=datetime(year=2020, month=6, day=20),
        serving_ttl="1d",
        schedule_interval="1d",
        data_lookback_period="7d"
    ),
    family='ad_serving',
    tags={'release': 'development', ':production': 'true'},
    owner="ravi@tecton.ai",
)