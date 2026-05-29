# KafkaFlow 🚀

**KafkaFlow** is a modern, high-performance, and visually stunning Web GUI client for Apache Kafka clusters. Built as a self-hosted alternative to desktop applications like *Offset Explorer* (formerly Kafka Tool), KafkaFlow runs as a single lightweight Spring Boot executable and allows anyone in your organization to browse topics, partitions, consumer groups, offsets, and messages directly from their web browser.

---

## Key Features

- **🌐 Web-Based & Self-Hosted**: Deploy once on a central Linux server; accessible to all teams instantly via browser.
- **✨ Premium Dark Aesthetics**: A gorgeous dashboard with glassmorphism, responsive charts, smooth hover animations, and cohesive indigo-purple themes.
- **🔒 Enterprise Security Ready**: Out-of-the-box dynamic support for:
  - **No Auth** (Local development)
  - **SASL / PLAIN** 
  - **SASL / SCRAM-SHA-256** & **SCRAM-SHA-512**
  - **SSL / TLS Encryption** (Client and Server certificates)
- **🔍 Advanced Message Browser**: 
  - Seek by **Oldest**, **Newest**, **Specific Offset**, or **Timestamp** (date picker).
  - High-density query grid displaying partition, offset, timestamp, record key, payload, and record headers.
  - Auto-deserialization of JSON and UTF-8 strings.
  - **Avro Deserialization** using Confluent Schema Registry (reads schema ID dynamically) OR **Manual Avro Schemas** (paste a local `.avsc` file).
- **🚀 Event Producer**: Form utility to send test messages containing keys, values, and custom headers to specific partitions.
- **📊 Consumer Group Monitor**: Active tracking of consumer groups, member client IDs, host IPs, partition assignments, and computed **LAG** per partition.
- **📦 Single Executable JAR**: React frontend and Spring Boot backend compile into a **single `.jar` file** (zero runtime Node.js or static server dependencies on production).

---

## Quick Start

### 1. Configure your Clusters
Edit `config.json` in the root folder to point to your Kafka clusters. You can define multiple clusters with different security protocols:

```json
{
  "port": 8080,
  "clusters": [
    {
      "id": "dev-cluster",
      "name": "Development Cluster (Plaintext)",
      "brokers": ["127.0.0.1:9092"],
      "schemaRegistry": "http://127.0.0.1:8081",
      "sasl": null,
      "ssl": null
    },
    {
      "id": "prod-sasl-scram",
      "name": "Production Test (SASL/SCRAM)",
      "brokers": ["kafka-1.company.org:9092", "kafka-2.company.org:9092"],
      "schemaRegistry": "https://registry.company.org",
      "sasl": {
        "mechanism": "SCRAM-SHA-512",
        "username": "developer-user",
        "password": "your-secure-password"
      },
      "ssl": null
    }
  ]
}
```

### 2. Build and Package
To build the frontend and compile it directly into the Spring Boot self-contained `.jar` executable:

```bash
# Step A: Compile React Frontend
cd frontend
npm install
npm run build
cd ..

# Step B: Package Spring Boot executable
mvn clean package
```

This creates `target/kafka-flow-1.0.0.jar`.

### 3. Run KafkaFlow
Run the standalone compiled package:

```bash
java -jar target/kafka-flow-1.0.0.jar
```

Open your browser and navigate to **`http://localhost:8080`**.

---

## Local Development (Hot Reloading)

If you are developing or customizing the frontend, you can run the backend and frontend separately to take advantage of hot module reloading (HMR):

1. **Start Backend (Port 8080)**: Run the Spring Boot application from your IDE or via command line.
2. **Start Frontend Dev Server (Port 3000)**:
   ```bash
   cd frontend
   npm run dev
   ```
3. Open **`http://localhost:3000`**. Vite will automatically proxy all API requests starting with `/api` to the Spring Boot backend running on `8080`.

---

## Linux Deployment (Systemd Guide)

To run KafkaFlow as a background system service on your Linux staging or testing server:

1. Copy `kafka-flow-1.0.0.jar` and your `config.json` into `/opt/kafka-flow/`.
2. Create a Systemd service descriptor at `/etc/systemd/system/kafka-flow.service`:

```ini
[Unit]
Description=KafkaFlow Web Service
After=syslog.target network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/kafka-flow
ExecStart=/usr/bin/java -jar kafka-flow-1.0.0.jar
Restart=always
RestartSec=10
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=kafka-flow

[Install]
WantedBy=multi-user.target
```

3. Enable and start the service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable kafka-flow
sudo systemctl start kafka-flow
```

Check status:
```bash
sudo systemctl status kafka-flow
```
