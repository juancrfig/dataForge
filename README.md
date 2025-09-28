# Data Forge

This project is a Proof-of-Concept for a data engineering pipeline that handles historical data migration and provides an API for real-time data ingestion, validation, backup, and restoration. 

# Project Features & Capabilities
This project implements a comprehensive data engineering pipeline with the following core features:
- **Automated Historical Data Migration**
  - An initial, one-time **ETL (Extract, Transform, Load)** process capable of migrating large volumes of historical data from local file storage into a cloud-based relational database.
- **Real-time Data Ingestion API**
  - **Unified Batch Ingestion Endpoint:** A single secure **RESTful API** endpoint designed to accept and process batches of 1 to 1,000 records in a single request, accommodating data for any of the database's tables.
  - **Robust Schema Enforcement & Error Handling:** The API automatically validates every incoming record against the target table's schema. Records that fail validation are rejected and logged for later analysis, ensuring that only clean, compliant data enters the database.
- **Data Backup & Recovery System**
  - **Scheduled Backups:** A feature to perform on-demand backups of any table. Data is saved to the file system in the highly efficient, column-oriented **Parquet** format, optimized for analytics and long-term storage.
  - **Target Restoration:** A function to restore the full contents of a specific table from a previously generated *Parquet* backup file, enabling rapid recovery.  

## Dataset Selection: Brazilian E-commerce Public Dataset by Olist

It was strategically chosen as the data source for this project because it provides a realistic and comprehensive environment to address the core requirements of data migration and ingestion. Its selection is based on three key characteristics:
- **Explicit Relational Schema:** The dataset is distributed across nine interconnected `.csv` files (e.g., `orders`, `customers`, `products`, `payments`). This structure directly simulates a real-world production database, making it the perfect canvas for designing and implementing a migration process that must respect **foreign key constraints** and relational integrity.
- **Real-World Data Complexity:** It contains a rich mix of data types, including strings, numbers, and timestamps, along with common inconsistencies found in real data. This presents a practical challenge for developing a robust **ETL** process that involves significant data cleaning, validation, and type casting.
- **Direct Alignment with Project Goals:** The multi-table, transactional nature of data is perfectly suited for demonstrating the project's primary objectives. It allows for a meaningful historical migration and tests the API's ability to ingest new, interconnected data points (like an order with its associated payment and item details) in a cohesive and reliable manner.

***

# Architecture

- **Cloud Provider (AWS):** Chosen based on industry prevalence.
- **Database (PostgreSQL on RDS):** Selected for its robustness, open-source nature, and strong relational capabilities.
- **API Framework (FastAPI):** Chosen over Flask for its automatic data validation via Pydantic and built-in interactive documentation, which directly addresses project requirements for data integrity and reduces boilerplate code.
- **Processing Module (Reusable):** A single processing module is used by both the historical load script and the API to ensure consistency in data validation and business logic.
- **Backup Format (Parquet):** Chosen for its columnar storage format, which offers high compression and efficient analytical querying, making it a superior choice for data warehousing and backup use cases compared to row-based formats like *CSV*.

## Tech Stack

- Python 3.13.3+
- FastAPI
- PostgreSQL
- Docker
- AWS RDS
- Pandas
- PyArrow (for Parquet)

### Prerequisites

- Docker
- AWS Account & configured CLI

### Instructions

1. **Clone the repository:** `git clone ...`
2. **Build the Docker container:** `docker build -t dataForge`
3. **Run the container:** `docker run`

***

## API Endpoints
