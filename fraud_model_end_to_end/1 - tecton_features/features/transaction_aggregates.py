from tecton import sql_transformation, TemporalAggregateFeaturePackage, DataSourceConfig, MaterializationConfig, FeatureAggregation
import entities as e
import data_sources as data_sources
from datetime import datetime


@sql_transformation(inputs=data_sources.transactions_batch_stream, has_context=True)
def transaction_aggregate_transformers(context, transactions_batch_stream):
    return f"""
    SELECT
        nameorig as user_id,
        1 as transaction,
        amount,
        timestamp
    FROM
        {transactions_batch_stream}
    """

transaction_aggregates = TemporalAggregateFeaturePackage(
    name="transaction_aggregates",
    description="Average transaction amount over a series of time windows ",
    entities=[e.user],
    transformation=transaction_aggregate_transformers,
    aggregation_slide_period="1h",
    aggregations=[
        FeatureAggregation(column="transaction", function="sum", time_windows=["1h", "12h", "24h","72h","168h", "960h"]),
        FeatureAggregation(column="amount", function="mean", time_windows=["1h", "12h", "24h","72h","168h", "960h"])],
    materialization=MaterializationConfig(
        online_enabled=True,
        offline_enabled=True,
        feature_start_time=datetime(2020, 12, 1),
    )
)
