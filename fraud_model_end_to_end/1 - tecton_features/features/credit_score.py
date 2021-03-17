from tecton import sql_transformation, TemporalFeaturePackage, DataSourceConfig, MaterializationConfig
import entities as e
import data_sources as data_sources
from datetime import datetime


@sql_transformation(inputs=data_sources.credit_scores_batch)
def credit_score_transformer(credit_scores_batch):
    return f"""
    SELECT
        user_id,
        credit_score,
        date as timestamp
    FROM
        {credit_scores_batch}
    """

users_credit_score = TemporalFeaturePackage(
    name="users_credit_score",
    description="A users credit score",
    transformation=credit_score_transformer,
    entities=[e.user],
    materialization=MaterializationConfig(
        online_enabled=True,
        offline_enabled=True,
        feature_start_time=datetime(2020, 12, 1),
        schedule_interval='7days',
        serving_ttl='21days',
    )
)
