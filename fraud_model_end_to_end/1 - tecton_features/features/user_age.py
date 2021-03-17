from tecton import sql_transformation, TemporalFeaturePackage, DataSourceConfig, MaterializationConfig
import entities as e
import data_sources as data_sources
from datetime import datetime

@sql_transformation(inputs=data_sources.fraud_users_batch, has_context=True)
def user_age_days_sql_transform(context, table_name):
    return f"""
        select
            user_id,
            CAST(datediff(to_date('{context.feature_data_end_time}'), dob) as INT) AS age,
            to_timestamp('{context.feature_data_end_time}') as timestamp
        from
            {table_name}
        """


user_age_days = TemporalFeaturePackage(
    name="user_age_days",
    description="Age of a user in days",
    transformation=user_age_days_sql_transform,
    entities=[e.user],
    materialization=MaterializationConfig(
        online_enabled=True,
        offline_enabled=True,
        feature_start_time=datetime(2020, 12, 1),
        schedule_interval='30days',
        serving_ttl='60days',
        data_lookback_period='90days'
    )
)
