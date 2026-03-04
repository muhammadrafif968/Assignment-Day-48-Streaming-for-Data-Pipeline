# Day 48 Streaming Data Pipeline Assignment

## 📁 Folder Structure

assignment/
│
├─ producer/
│  └─ producer.py
├─ streaming/
│  └─ spark_streaming_job.py
├─ docker-compose.yml
└─ README.md

## 🐳 Docker Setup

Docker Compose sudah include:
- **Zookeeper** – `confluentinc/cp-zookeeper:7.4.0`
- **Kafka Broker** – `confluentinc/cp-kafka:7.4.0`
- **Kafka UI** – `provectuslabs/kafka-ui:latest`
- **Jupyter Spark Notebook** – `jupyter/pyspark-notebook:latest`

### Start Docker

docker-compose up -d  
docker ps

## 📝 Kafka Topics

Buat 3 topik:

docker exec -it kafka bash  
kafka-topics --bootstrap-server kafka:9092 --create --topic transactions --partitions 1 --replication-factor 1  
kafka-topics --bootstrap-server kafka:9092 --create --topic transactions_valid --partitions 1 --replication-factor 1  
kafka-topics --bootstrap-server kafka:9092 --create --topic transactions_dlq --partitions 1 --replication-factor 1

## 🏭 Event Producer

Jalankan producer untuk mengirim event transaksi:

docker exec -it jupyter-spark bash  
python /home/jovyan/producer/producer.py

- Mengirim event setiap 1–2 detik  
- Event jenis: valid, invalid, late  
- Minimal ada 1 duplicate event  
- Late events >3 menit → simulasi watermark  

## ⚡ Spark Streaming Job

Jalankan Spark Streaming:

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /home/jovyan/streaming/spark_streaming_job.py

**Fitur Streaming Job:**
- Membaca dari Kafka topic `transactions`
- Deserialize JSON ke DataFrame dengan schema terdefinisi
- 5 Validasi wajib:
  1. Mandatory field (`user_id`, `amount`, `timestamp`)
  2. Type validation
  3. Range validation (`amount` 1–10.000.000)
  4. Source validation (`mobile`, `web`, `pos`)
  5. Duplicate detection (`user_id` + `timestamp`)
- Tambahkan kolom:
  - `is_valid` → True/False
  - `error_reason` → alasan invalid
- Routing output:
  - Data valid → `transactions_valid`
  - Data invalid → `transactions_dlq`
- Watermark: `.withWatermark("event_time", "3 minutes")`
- Tumbling window 1 menit → hitung `running_total` valid  

## 📊 Kafka UI (Opsional)

Buka Kafka UI: http://localhost:8080  

- Lihat topic:
  - `transactions_valid` → event valid
  - `transactions_dlq` → event invalid / late  

## 🗂 GitHub Submission

Folder minimal:
- /producer/producer.py  
- /streaming/spark_streaming_job.py  
- docker-compose.yml  
- README.md  