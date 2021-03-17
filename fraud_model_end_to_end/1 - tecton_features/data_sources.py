from tecton import (
    VirtualDataSource,
    HiveDSConfig,
    KinesisDSConfig
)
from tecton_spark.function_serialization import inlined

@inlined
def raw_data_deserialization(df):
    from pyspark.sql.functions import col

    PAYLOAD_SCHEMA = (
      StructType()
            .add("nameorig", StringType(), False)
            .add("amount", DoubleType(), False)
    )

    EVENT_ENVELOPE_SCHEMA = (
      StructType()
            .add("timestamp", TimestampType(), False)
            .add("payload", PAYLOAD_SCHEMA, False)
  )

    df = df.withColumn("user_id", col("event.payload.nameorig"))
    df = df.withColumn("transaction", 1)
    df = df.withColumn("amount", col("event.payload.amount"))
    df = df.withColumn("timestamp", col("event.timestamp"))

    return df

transactions_batch_stream = VirtualDataSource(
    name="transactions_batch_stream",
    batch_ds_config=HiveDSConfig(
        database='fraud',
        table='fraud_transactions_pq',
        timestamp_column_name='timestamp',
    ),
    stream_ds_config=KinesisDSConfig(
        stream_name='',
        region='us-west-2',
        # kafka_bootstrap_servers='kafka-bus.example.com:9092',
        topics='transaction_events',
        timestamp_key='timestamp',
        raw_stream_translator=raw_data_deserialization
    )
)

fraud_users_batch = VirtualDataSource(
    name="fraud_users_batch",
    batch_ds_config=HiveDSConfig(
        database='fraud',
        table='fraud_users_pq'
    )
)

credit_scores_batch = VirtualDataSource(
    name="credit_scores_batch",
    batch_ds_config=HiveDSConfig(
        database='fraud',
        table='fraud_credit_scores_pq',
    )
)
