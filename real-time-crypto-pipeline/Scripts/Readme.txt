📌 Problem Statement

In fast-moving markets like cryptocurrency, prices change every second. Businesses need a real-time pipeline that can:
Fetch data continuously from an API.
Stream it reliably for downstream systems.
Load into a data warehouse for reporting & analytics.

The challenge: Building this end-to-end data pipeline using only free, open-source tools and integrating with Snowflake (free trial) for analytics.

⚙️ Solution & Architecture

I designed and implemented a real-time streaming pipeline:

Data Ingestion (Producer)

A Python Kafka Producer fetches Bitcoin & Ethereum prices from the CoinGecko API
.

Messages are pushed into a Kafka topic (crypto_prices).

Streaming & Orchestration

Kafka acts as the message broker.

Apache Airflow (standalone) orchestrates DAGs to manage producer and consumer tasks.

Data Consumption & Loading

A Kafka Consumer reads messages from the topic.

Data is inserted into a Snowflake table (crypto_prices) with additional metadata:

dag_run_id (to track pipeline runs)

source (producer/consumer identifier)

Storage & Analytics

Snowflake stores the ingested data in a staging schema.

Queries & reports were created in Snowsight (Snowflake UI) to visualize and analyze price trends.

🏗️ Tech Stack

Python (API calls, Kafka producer/consumer)

Apache Kafka & Zookeeper (event streaming, Dockerized)

Apache Airflow (workflow orchestration)

Snowflake (data warehouse & reporting)

Snowsight (built-in BI tool for visualization)

Docker (Kafka/Zookeeper containerized setup)

📊 Results

Built a working real-time pipeline that streams crypto data every minute.

Successfully loaded data into Snowflake staging tables with metadata for tracking.

Created basic BI reports on crypto price trends using Snowsight.

Improved my skills in data streaming, orchestration, and cloud warehouses.

🚧 Challenges Faced & Solutions

Airflow Standalone User Issue

Airflow initially didn’t create a default admin account due to Kafka/Zookeeper conflicts.

✅ Fixed by restarting Airflow from scratch and reinitializing the environment.

Docker Containers Exiting

Kafka/Zookeeper containers stopped unexpectedly.

✅ Resolved by cleaning up conflicting containers and restarting with a custom Docker network (kafka-network).

Package Compatibility

snowflake-connector-python required a compatible version of cffi.

✅ Fixed by downgrading to the correct version and reinstalling dependencies in the venv.

Duplicate Data

CoinGecko API had rate limits → duplicate values appeared in Snowflake.

✅ Handled via query filtering & metadata columns in Snowflake (dag_run_id, source).

📷 Project Artifacts

Code Folder: Python producer/consumer scripts + Airflow DAGs.

Screenshots Folder: Terminal logs, Airflow UI, Docker network setup, Snowflake Snowsight reports.

Output Logs Folder: Saved outputs from terminal commands (Kafka messages, Airflow logs).

🚀 Key Takeaways

Gained hands-on experience with streaming pipelines using Kafka & Airflow.

Learned to debug real-world deployment issues (Docker, Airflow, Snowflake connector).

Showcased end-to-end project ownership — from ingestion to reporting.