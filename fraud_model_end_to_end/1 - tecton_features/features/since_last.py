from tecton import sql_transformation, TemporalFeaturePackage, DataSourceConfig, MaterializationConfig
import entities as e
import data_sources as data_sources
from datetime import datetime


@sql_transformation(inputs=data_sources.transactions_batch_stream, has_context=True)
def days_since_last_transformer(context, transactions_batch_stream):
    return f"""
        select
            nameorig as user_id,
            datediff(to_date('{context.feature_data_end_time}'), max(to_date(timestamp))) as days_since_last,
            to_timestamp('{context.feature_data_end_time}') as timestamp
        from
            {transactions_batch}
        GROUP BY
            nameorig
    """


days_since_last_transaction = TemporalFeaturePackage(
    name="days_since_last_transaction",
    description="[SQL Feature] Number of days since a users last transaction",
    transformation=days_since_last_transformer,
    entities=[e.user],
    materialization=MaterializationConfig(
        online_enabled=True,
        offline_enabled=True,
        feature_start_time=datetime(2020, 12, 1),
        schedule_interval='1day',
        serving_ttl='1day',
        data_lookback_period='90days'
    )
)
