from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
import traceback
import os

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'weather_data')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
HDFS_DIR = "/weather_data/alerts"
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE', 'hdfs-namenode:9000')
HDFS_CLIENT = InsecureClient(f"http://{HDFS_NAMENODE.split(':')[0]}:9870", user="root")

def read_once_and_upload():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000
    )
    
    try:
        msg = next(consumer)
    except StopIteration:
        print("No messages in Kafka topic.")
        consumer.close()
        return
    
    record = msg.value
    consumer.close()
    
    # Filtrer uniquement les alertes (windspeed > 10)
    if record.get('windspeed', 0) <= 10:
        print(f"No alert: windspeed={record.get('windspeed')} <= 10")
        return
    
    # Ajouter un flag d'alerte
    record['alert'] = True
    record['alert_type'] = 'high_wind'
    
    # Sérialiser
    json_data = json.dumps(record, indent=2)
    
    # Créer un nom de fichier horodaté
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"alert_{ts}.json"
    hdfs_path = f"{HDFS_DIR}/{filename}"
    
    try:
        print(f"Creating directory and writing to: {hdfs_path}")
        HDFS_CLIENT.makedirs(HDFS_DIR)
        
        # Écrire directement, pas de fichier local
        with HDFS_CLIENT.write(hdfs_path, overwrite=False, encoding="utf-8") as writer:
            writer.write(json_data)
        
        print(f"✅ Upload successful: {hdfs_path}")
        print(f"📊 Alert data: {record}")
    except Exception:
        print("❌ HDFS ERROR:")
        traceback.print_exc()
        raise

with DAG(
    dag_id="weather_alert_to_hdfs",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['weather', 'kafka', 'hdfs'],
) as dag:
    read_once = PythonOperator(
        task_id="consume_and_save_alert",
        python_callable=read_once_and_upload,
    )
    
    read_once
