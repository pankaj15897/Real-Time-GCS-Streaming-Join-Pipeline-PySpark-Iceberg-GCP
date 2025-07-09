# order_payment_producer_gcs.py
import json
import random
import time
import uuid
from datetime import datetime, timedelta

from google.cloud import storage

BUCKET_NAME    = "file-stream-bucket"
ORDERS_PREFIX  = "stream/orders"    # will write to gs://…/stream/orders/YYYY_MM_DD/
PAYMENTS_PREFIX= "stream/payments"  # will write to gs://…/stream/payments/YYYY_MM_DD/

# instantiate once
_client = storage.Client()
_bucket = _client.bucket(BUCKET_NAME)

def generate_order(order_id):
    return {
        "order_id":    order_id,
        "order_date":  (datetime.utcnow() - timedelta(minutes=random.randint(0,30))).isoformat(),
        "created_at":  datetime.utcnow().isoformat(),
        "customer_id": f"customer_{random.randint(1,100)}",
        "amount":      random.randint(100,1000)
    }

def generate_payment(order_id):
    return {
        "payment_id":   str(uuid.uuid4()),
        "order_id":     order_id,
        "payment_date": (datetime.utcnow() - timedelta(minutes=random.randint(0,30))).isoformat(),
        "created_at":   datetime.utcnow().isoformat(),
        "amount":       random.randint(50,500)
    }

def main():
    order_counter = 1
    while True:
        #–– generate & write the order
        order_id = f"order_{order_counter}"
        order = generate_order(order_id)
        ts = int(time.time() * 1e3)
        date_str = datetime.utcnow().strftime("%Y_%m_%d")

        order_blob = (
            f"{ORDERS_PREFIX}/{date_str}/"
            f"order_{order_counter}_{ts}.json"
        )
        _bucket.blob(order_blob).upload_from_string(
            json.dumps(order), content_type="application/json"
        )
        print(f"Wrote order → gs://{BUCKET_NAME}/{order_blob}")

        #–– generate & write the matching payment
        payment     = generate_payment(order_id)
        payment_blob= (
            f"{PAYMENTS_PREFIX}/{date_str}/"
            f"payment_{order_counter}_{ts}.json"
        )
        _bucket.blob(payment_blob).upload_from_string(
            json.dumps(payment), content_type="application/json"
        )
        print(f"Wrote payment → gs://{BUCKET_NAME}/{payment_blob}")

        order_counter += 1
        time.sleep(3)   # wait before next pair

if __name__ == "__main__":
    main()