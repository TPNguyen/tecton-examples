from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, TimestampType
from tecton import Entity, PushFeaturePackage, MaterializationConfig, DeltaConfig, FeatureService

user_entity = Entity(name="user", default_join_keys=["userid"])

schema = StructType()
schema.add(StructField("timestamp", TimestampType()))
schema.add(StructField("userid", StringType()))
schema.add(StructField("num_purchases", LongType()))

fp = PushFeaturePackage(
    name="user_purchases_push_fp",
    entities=[user_entity],
    schema=schema,
    materialization=MaterializationConfig(
        serving_ttl="30d",
        offline_enabled=True,
        offline_config=DeltaConfig(),
        online_enabled=True,
    ),
)
