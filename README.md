# Iceberg with Dremio

This repository provides a setup for integrating **Apache Iceberg** with **Dremio** for efficient data lake operations.

## 🚀 Overview
Apache Iceberg is an open table format that brings **ACID transactions** to big data workloads, enabling efficient querying and data management. This repository demonstrates how to use **Dremio** to interact with Iceberg tables.

## 📌 Prerequisites
Ensure you have the following installed:
- **Dremio (Enterprise or Community Edition)**
- **Apache Iceberg**
- **Docker & Docker Compose** (if using containerized deployment)
- **Python 3.8+** (for optional scripting)
- **Git**

## 🛠️ Installation & Setup
### 1️⃣ Clone this repository
```bash
git clone https://github.com/jayusman/iceberg-dremio.git
cd iceberg-dremio
```

### 2️⃣ Start Dremio
If you are running Dremio in Docker, use:
```bash
docker-compose up -d
```
Or start it manually if installed locally.

### 3️⃣ Configure Dremio to Use Iceberg
Modify the `dremio.conf` file:
```yaml
services: {
  coordinator.enabled: true,
  executor.enabled: true,
  flight.use_session_service: true
}
```
Restart Dremio after making changes.

### 4️⃣ Verify Iceberg Tables in Dremio
Run the following SQL query in Dremio:
```sql
SHOW TABLES IN nessie.spotify;
```

## 🔗 Connecting Dremio to Apache Iceberg
Use the following **SQLAlchemy URI** for connecting **Apache Superset** or other tools:
```plaintext
dremio+flight://host.docker.internal:9047/?Authentication=Plain&Username=<YOUR_USERNAME>&Password=<YOUR_PASSWORD>
```

## 🔥 Running Queries
Create an Iceberg table:
```sql
CREATE TABLE nessie.spotify.tracks (
    spotify_id STRING,
    name STRING,
    artists STRING,
    daily_rank INT,
    country STRING,
    snapshot_date STRING,
    popularity INT
) USING iceberg;
```

Insert data:
```sql
INSERT INTO nessie.spotify.tracks VALUES ('123', 'Song Name', 'Artist Name', 1, 'US', '2024-03-12', 95);
```

## 🏗️ Contributing
Feel free to fork this repository, submit issues, or contribute by sending pull requests.

## 📄 License
This project is licensed under the **MIT License**.

---
**Author:** [Jayusman](https://github.com/jayusman)

Happy coding! 🚀

