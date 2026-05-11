import pandas as pd
from kafka import KafkaProducer
import json
import time
import os
import glob

# Yapılandırma
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = 'taxi-topic'
DATA_PATH = '/data/yellow_tripdata_2026-01.parquet'
BATCH_SIZE = 100 # Her adımda kaç satır gönderilecek
DELAY = 1 # Gönderimler arası saniye bazında gecikme

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def create_topic_if_not_exists():
    # Kafka broker'ın hazır olması için kısa bir bekleme
    time.sleep(10)
    print(f"Kafka Broker {KAFKA_BROKER} bağlantısı kuruluyor...")
    
def get_producer():
            return KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=json_serializer,
                acks='all'
            )

def run_producer():
    print(f"Veri dosyası okunuyor: {DATA_PATH}")
    
    # Parquet dosyasını oku
    try:
        # 1 Milyon satır sınırı ve hız için optimizasyon
        # head(1000000) ile sadece ilk 1 milyon kaydı alıyoruz
        df = pd.read_parquet(DATA_PATH).head(1000000)
        print(f"Toplam {len(df)} kayıt okundu. Akış başlıyor...")
    except Exception as e:
        print(f"HATA: Dosya okunamadı: {e}")
        return

    producer = get_producer()
    print(f"Kafka'ya veri akışı başlıyor... Topic: {TOPIC_NAME}")

    counter = 0
    # Veriyi BATCH_SIZE (100) gruplar halinde göndererek sistemi yormuyoruz
    for i in range(0, len(df), BATCH_SIZE):
        batch_df = df.iloc[i : i + BATCH_SIZE]
        messages = batch_df.to_dict('records')
        
        for message in messages:
            # BELGEDE İSTENEN ZORUNLU ALANLARIN EKLENMESİ
            message['timestamp'] = message['tpep_pickup_datetime'].isoformat() if hasattr(message['tpep_pickup_datetime'], 'isoformat') else str(message['tpep_pickup_datetime'])
            message['kullanici_id'] = f"USER_{message.get('VendorID', '0')}_{counter}"
            message['olay_tipi'] = "taxi_trip"
            message['ilgili_id'] = str(message.get('PULocationID', '0'))
            
            # Timestamp objelerini string'e çevir
            for key, value in message.items():
                if hasattr(value, 'isoformat') and not isinstance(value, str):
                    message[key] = value.isoformat()
            
            # Kafka'ya gönder
            producer.send(TOPIC_NAME, value=message)
            counter += 1
            
        # Her batch sonunda bekleme ve loglama
        print(f"[{time.strftime('%H:%M:%S')}] Toplam {counter} mesaj gönderildi.")
        time.sleep(DELAY)

    producer.flush()
    print("Veri akışı tamamlandı.")

if __name__ == "__main__":
    run_producer()
