# stateful_gcs_to_bq.py

import pandas as pd, logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.streaming.state import GroupStateTimeout

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("StatefulGCS")

# 1) boot Spark and pull in GCS + BigQuery connectors
spark = (
    SparkSession.builder
      .appName("StatefulGCSOrdersPayments_Iceberg")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.hadoop_catalog","org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
      .config("spark.sql.catalog.hadoop_catalog.warehouse",
              "gs://spark-iceberg-warehouse/")
      .getOrCreate()
)

# 2) DEFINE SCHEMAS
order_schema = StructType([
  StructField("order_id",    StringType()),
  StructField("order_date",  StringType()),
  StructField("created_at",  StringType()),
  StructField("customer_id", StringType()),
  StructField("amount",      IntegerType())
])
payment_schema = StructType([
  StructField("payment_id",   StringType()),
  StructField("order_id",     StringType()),
  StructField("payment_date", StringType()),
  StructField("created_at",   StringType()),
  StructField("amount",       IntegerType())
])

# 3) STREAM NEW ORDER FILES
orders = (
  spark.readStream
    .schema(order_schema)
    .option("maxFilesPerTrigger", 1) 
    .json("gs://file-stream-bucket/stream/orders/*/order_*")
    .withColumn("type", lit("order"))
)
logger.info("Orders stream ready")

# 4) STREAM NEW PAYMENT FILES
payments = (
  spark.readStream
    .schema(payment_schema)
    .option("maxFilesPerTrigger", 1) 
    .json("gs://file-stream-bucket/stream/payments/*/payment_*")
    .withColumn("type", lit("payment"))
)
logger.info("Payments stream ready")

# 5) UNION THEM
stream = orders.unionByName(payments, allowMissingColumns=True)

# 6) YOUR STATEFUL PANDAS-IN-SPARK LOGIC
def proc(key, pdfs, state):
    (order_id,) = key
    if state.exists:
        od, ca, cid, amt = state.get
    else:
        od = ca = cid = amt = None

    out = []
    for pdf in pdfs:
        for r in pdf.itertuples(index=False):
            if r.type=="order":
                if not state.exists:
                    state.update((r.order_date, r.created_at,
                                  r.customer_id, r.amount))
                    state.setTimeoutDuration(15*60*1000)
                    od, ca, cid, amt = (r.order_date, r.created_at,
                                        r.customer_id, r.amount)
                else:
                    logger.warning(f"dup order {order_id}")
            else:  # payment
                if state.exists:
                    out.append({
                      "order_id": order_id,
                      "order_date": od,
                      "created_at": ca,
                      "customer_id": cid,
                      "order_amount":  amt,
                      "payment_id":    r.payment_id,
                      "payment_date":  r.payment_date,
                      "payment_amount": r.amount
                    })
                    state.remove()
                else:
                    logger.warning(f"orphan pay {order_id}")
    if state.hasTimedOut:
        logger.warning(f"timed out {order_id}")
        state.remove()
    if out:
        yield pd.DataFrame(out)

result = stream.groupBy("order_id") \
    .applyInPandasWithState(
      func=proc,
      outputStructType="""
        order_id STRING,
        order_date STRING,
        created_at STRING,
        customer_id STRING,
        order_amount INT,
        payment_id STRING,
        payment_date STRING,
        payment_amount INT
      """,
      stateStructType="""
        order_date STRING,
        created_at STRING,
        customer_id STRING,
        amount INT
      """,
      outputMode="append",
      timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
    )


# debug_q = (
#     result.writeStream
#           .format("console")
#           .outputMode("append")
#           .option("truncate", False)
#           .start()
# )

# debug_q.awaitTermination()

# 7) WRITE INTO Iceberg
query = (
    result.writeStream
          .format("iceberg")
          .option("checkpointLocation",
                  "gs://spark-iceberg-warehouse/checkpoints/orders_payments")
          .toTable("hadoop_catalog.default.orders_payments")
)

query.awaitTermination()