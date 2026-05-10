# 🚖 TLC Streaming Data Pipeline & ML Prediction

Bu proje, NYC Yellow Taxi veri setini kullanarak uçtan uca bir **Büyük Veri (Big Data)** ve **Makine Öğrenmesi (Machine Learning)** boru hattı oluşturmaktadır. Proje; veri üretimi, gerçek zamanlı işleme, depolama ve modelleme aşamalarını kapsamaktadır.

## 🏗️ Sistem Mimarisi

Proje, Docker üzerinde çalışan mikroservis mimarisine sahiptir:

1.  **Kafka & Zookeeper:** Veri akışını yöneten mesaj kuyruğu.
2.  **Apache Spark:** Verileri gerçek zamanlı (streaming) olarak işleyen motor.
3.  **Delta Lake:** Verilerin Bronze, Silver ve Gold katmanlarında saklandığı modern veri gölü (Data Lakehouse).
4.  **MLflow:** Deney takibi ve model yönetimi platformu.
5.  **Jupyter Lab:** Geliştirme ve analiz arayüzü.

## 🚀 Çalıştırma Adımları

### 1. Ortamı Hazırlama
Tüm servisleri başlatmak için terminalde proje dizinindeyken şu komutu çalıştırın:
```bash
docker-compose up -d
```

### 2. Veri Boru Hattını Başlatma
Jupyter Lab arayüzüne (`localhost:8888`) gidin (Şifre: `bigdata`) ve şu sırayla notebook'ları çalıştırın:

1.  **`2_Spark_Streaming.ipynb`**: Spark alıcısını (consumer) başlatır.
2.  **`1_Producer.ipynb`**: Kafka'ya veri akışını (producer) başlatır.
3.  **`3_EDA.ipynb`**: Veri seti üzerinde keşifsel analiz yapar.
4.  **`4_Feature_Engineering.ipynb`**: Verileri modellemeye hazır hale getirir.
5.  **`5_Model_Training.ipynb`**: MLflow entegrasyonu ile model eğitir.
6.  **`6_Visualization.ipynb`**: Tüm sonuçları dashboard olarak görselleştirir.

## 📊 Kullanılan Teknolojiler

*   **Dil:** Python
*   **İşleme:** PySpark (Structured Streaming)
*   **Depolama:** Delta Lake
*   **Model Takibi:** MLflow
*   **Konteynerleştirme:** Docker & Docker Compose

## 📈 Dashboard ve Görselleştirme
Proje sonunda elde edilen veriler üzerinden; saatlik ücret trendleri, mesafe-ücret ilişkisi ve model performans metrikleri (RMSE, MAE, R2) interaktif grafiklerle sunulmaktadır.

---
