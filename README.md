# Real-Time Log Processing Pipeline

This project demonstrates a complete real-time streaming pipeline using **Kafka**, **PySpark Structured Streaming**, **Delta Lake**, and **Python**. It simulates log generation, streams data through Kafka, processes it using Spark, and stores the results in Delta format.

---

## 🚀 Project Architecture

**Data Flow:**

```
JSON Log Generator → Kafka Producer → Kafka Broker → Spark Streaming → Delta Lake Storage
```

---

## 🧩 Components Used

### **1. Python**

Used for:

* JSON log generation
* Kafka producers
* Spark consumer code

### **2. Kafka (Confluent Platform 7.4.0)**

* Broker version: **Kafka 3.4.x**
* Zookeeper version: **3.8.x**
* Managed via Docker Compose

### **3. PySpark (3.3.3)**

* Used for Structured Streaming
* Consumes data from Kafka
* Writes to Delta Lake

### **4. Delta Lake (2.4.0)**

* Compatible with Spark **3.3.3**
* Used for ACID storage and incremental data processing

---

## 📦 Folder Structure

```
Real-Time-Log-Processing/
│
├── docker-compose.yml
├── generate_random_json.py
├── consumer_spark.py
├── producer.py     
├── requirements.txt
└── README.md
```

## 📝 Notes

* Ensure correct Delta version for Spark 3.3.3 → **2.3.0**
* Remove conflicting Spark jars from `~/.ivy2/jars` if needed
* Kafka must be running before Spark Streaming starts
* Use a separate terminal window for each component
