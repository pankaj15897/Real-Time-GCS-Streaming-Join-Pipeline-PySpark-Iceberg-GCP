## 🚀 Real-Time GCS Streaming Join Pipeline | PySpark + Iceberg + GCP

A real-time data pipeline that simulates **order and payment events**, streams them from **Google Cloud Storage**, performs **stateful joins** using **PySpark Structured Streaming**, and writes enriched output to an **Apache Iceberg table** for durable, queryable storage.

---

### 🧰 Tech Stack

| Component                      | Purpose                                      |
| ------------------------------ | -------------------------------------------- |
| PySpark (Structured Streaming) | Real-time data ingestion and join logic      |
| Pandas UDF with Stateful API   | Join stateful order-payment pairs            |
| Apache Iceberg                 | Storage format with ACID and time travel     |
| GCS (Cloud Storage)            | Acts as both input stream and warehouse sink |
| Google Cloud Platform          | Storage, orchestration, and scaling          |

---

### 🧪 Project Components

```bash
├── producer.py        # Simulates and streams JSON orders/payments to GCS
├── streaming_job.py   # PySpark structured streaming job with Iceberg sink
```

---

### 📈 Pipeline Overview

1. **Event Generator (`producer.py`)**:

   * Simulates `order` and `payment` JSON events every 3 seconds.
   * Writes each file to:

     ```
     gs://file-stream-bucket/stream/orders/YYYY_MM_DD/
     gs://file-stream-bucket/stream/payments/YYYY_MM_DD/
     ```

2. **Stream Processor (`streaming_job.py`)**:

   * Reads JSON files in real-time from GCS
   * Parses order and payment streams
   * Joins them **statefully** using `order_id` as key
   * Handles:

     * Duplicate orders
     * Orphan payments
     * Timeout orders (after 15 minutes)
   * Writes final enriched records to an **Iceberg table**:

     ```
     gs://spark-iceberg-warehouse/
     → hadoop_catalog.default.orders_payments
     ```

---

### 🧠 Stateful Join Logic

* Uses `applyInPandasWithState` for grouping by `order_id`
* Remembers incoming orders and waits for a matching payment
* Emits joined record once both sides are available
* Automatically removes state if:

  * The join completes
  * Timeout is reached
  * Duplicates detected

---

### 🧾 Output Schema

```text
order_id        STRING
order_date      STRING
created_at      STRING
customer_id     STRING
order_amount    INT
payment_id      STRING
payment_date    STRING
payment_amount  INT
```

---

### 📦 Iceberg Configuration (via Spark)

```python
.config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
.config("spark.sql.catalog.hadoop_catalog.warehouse", "gs://spark-iceberg-warehouse/")
```

Output table:

```
hadoop_catalog.default.orders_payments
```

---

### 🧪 Run Instructions

#### ✅ 1. Generate Streaming Events

```bash
python producer.py
```

This will continuously write order and payment JSONs to your configured GCS bucket.

#### ✅ 2. Launch Spark Job

Make sure you have Iceberg JARs and GCS connector available in Spark cluster.

```bash
spark-submit streaming_job.py
```

The pipeline will:

* Read files in real-time
* Join and enrich
* Write to Iceberg table with checkpointing

---

### 📌 Features

* ✅ **GCS as streaming source** — no Kafka/SQS required
* ✅ **Real-time stateful joins** using PySpark
* ✅ **Robust data modeling** with Iceberg tables
* ✅ **Handles late/early/missing events**
* ✅ **Auto cleanup with GroupStateTimeout**
* ✅ **Pluggable schemas and storage layers**

---

### 👨‍💻 Author

**Pankaj** — GCP Data Engineer
🛠️ Focused on real-time pipelines, Spark + GCS, and Iceberg integration.

---
