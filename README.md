# KafkaFlow 🚀

[English](#english) | [中文说明](#中文说明)

---

# English

**KafkaFlow** is a modern, high-performance, and visually stunning Web GUI client for Apache Kafka clusters. Built as a self-hosted alternative to desktop applications like *Offset Explorer* (formerly Kafka Tool), KafkaFlow runs as a single lightweight Spring Boot executable and allows anyone in your organization to browse topics, partitions, consumer groups, offsets, and messages directly from their web browser.

---

## 🌟 Key Features

- **🌐 Web-Based & Self-Hosted**: Deploy once on a central Linux server; accessible to all teams instantly via browser.
- **✨ Premium Dark Aesthetics**: A gorgeous dashboard with glassmorphism, responsive charts, smooth hover animations, and cohesive indigo-purple themes.
- **🔍 Dual-Layer Message Search**:
  - **Local Instant Filter**: Type in the revamped table header search bar to instantly filter cached message grids.
  - **Server-Side Deep Scan**: Type a keyword in the "Server Search Keyword" field to let the backend poll and scan up to 30,000 broker records dynamically (matching keys, payloads, offsets, or headers).
- **🔒 Enterprise Security Ready**: Out-of-the-box dynamic support for:
  - **No Auth** (Local development)
  - **SASL / PLAIN**, **SASL / SCRAM-SHA-256** & **SCRAM-SHA-512**
  - **SASL / GSSAPI (Kerberos)**:
    - **Keytab File Uploads**: Upload keytabs dynamically. Files are saved in **isolated UUID subdirectories** (e.g. `./keytabs/UUID/file.keytab`) to prevent multi-cluster name collisions and overwrites.
    - **Ticket Cache (kinit)**: Connect securely using server-level Active Directory TGT ticket caches.
    - **System Default (JVM / OS)**: Completely bypasses programmatic JAAS injection. Lets the client automatically fall back to JVM arguments like `-Djava.security.auth.login.config` or environmental settings.
  - **SSL / TLS Encryption** (Client and Server certificates).
- **📦 Single Executable JAR**: React frontend and Spring Boot backend compile into a **single `.jar` file** (zero runtime Node.js or static server dependencies on production).
- **📊 Consumer Group Monitor**: Active tracking of consumer groups, member client IDs, host IPs, partition assignments, and computed **LAG** per partition.

---

## 🚀 Quick Start

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

## 🛠️ Local Development (Hot Reloading)

If you are developing or customizing the frontend, you can run the backend and frontend separately to take advantage of hot module reloading (HMR):

1. **Start Backend (Port 8080)**: Run the Spring Boot application from your IDE or via command line.
2. **Start Frontend Dev Server (Port 3000)**:
   ```bash
   cd frontend
   npm run dev
   ```
3. Open **`http://localhost:3000`**. Vite will automatically proxy all API requests starting with `/api` to the Spring Boot backend running on `8080`.

---

# 中文说明

**KafkaFlow** 是一款现代、高效且极具美学设计的 Apache Kafka 网页版可视化客户端。作为本地桌面应用（如 *Offset Explorer*）的极佳轻量化替代品，KafkaFlow 运行在一个单文件 Spring Boot 可执行程序中，支持您组织内的任何人通过浏览器直接查看和操作 Kafka 集群的 Topics、Partitions、消费者组（Consumer Groups）、偏移量（Offsets）以及消息内容。

---

## 🌟 核心特性

- **🌐 网页版 & 集中部署**：一次部署在公司测试机或 Linux 服务器，团队成员无需安装任何客户端，用浏览器即可立即访问。
- **✨ 极奢暗黑美学**：精心调配的 HSL 渐变暗黑视效，结合磨砂玻璃质感（Glassmorphism）、微交互动画和响应式数据图表，告别传统工具的简陋粗糙感。
- **🔍 双层消息检索**：
  - **前端即时过滤**：在重构后的宽大消息表头搜索框中直接输入，即可瞬间对已加载消息的 Payload 开展实时本地过滤。
  - **后端深度扫码**：在检索控制台输入 "Server Search Keyword"（服务侧搜索关键字）并点击 Consume，后端将深度 poll 并遍历最多 30,000 条 Broker 历史消息（支持 Key、Value 载荷、Offset 以及 Header 头信息的模糊匹配）。
- **🔒 企业级安全套件支持**：
  - **No Auth** (本地开发免密连接)
  - **SASL / PLAIN**, **SASL / SCRAM-SHA-256** & **SCRAM-SHA-512**
  - **SASL / GSSAPI (Kerberos 认证)**：
    - **Keytab 证书隔离上传**：支持从网页直接上传 Keytab 凭证，并在后端自动为每个集群生成 **UUID 随机隔离目录** 存储（例如 `./keytabs/UUID/file.keytab`），彻底杜绝同名 Keytab 覆盖冲突。
    - **操作系统 Ticket Cache (kinit)**：支持直接利用服务器操作系统的活动目录 TGT 缓存进行免密认证。
    - **System Default (JVM / 操作系统级托管)**：完全绕过代码层面的 JAAS 组装，直接回退并继承 JVM 启动参数（如 `-Djava.security.auth.login.config`）配置的全局 JAAS 凭证。
  - **SSL / TLS 加密传输**（支持客户端及服务端的 Truststore/Keystore 安全证书加密）。
- **📦 单可执行文件编译**：React 前端构建物会直接嵌入到 Spring Boot 的 `static` 目录中，最终只编译生成**单个独立的 `.jar` 文件**，无任何 Node.js 运行时或额外静态服务器依赖。
- **📊 消费者组（Consumer Groups）监控**：实时监控所有活跃的消费者组、成员 Client ID、客户端 IP、分区配给情况，并直观呈现每个 Partition 上的 **LAG** 积压积滞数据。

---

## 🚀 快速启动

### 1. 配置您的集群
在项目根目录编辑 `config.json`，指向您的 Kafka 集群。您可在此定义拥有不同安全协议的多个集群：

```json
{
  "port": 8080,
  "clusters": [
    {
      "id": "dev-cluster",
      "name": "开发环境集群 (明文)",
      "brokers": ["127.0.0.1:9092"],
      "schemaRegistry": "http://127.0.0.1:8081",
      "sasl": null,
      "ssl": null
    },
    {
      "id": "prod-sasl-scram",
      "name": "线上测试集群 (SASL/SCRAM)",
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

### 2. 编译与打包
通过以下步骤编译前端并打包进 Spring Boot 可执行 `.jar` 文件中：

```bash
# 步骤 A：编译 React 前端
cd frontend
npm install
npm run build
cd ..

# 步骤 B：打包 Spring Boot
mvn clean package
```

这将在项目根目录下生成可执行文件 `target/kafka-flow-1.0.0.jar`。

### 3. 运行 KafkaFlow
运行独立的可执行文件包：

```bash
java -jar target/kafka-flow-1.0.0.jar
```

打开您的浏览器并访问 **`http://localhost:8080`**。

---

## 🛠️ 本地开发环境（热重载）

如果您正在定制前端或开发新功能，可以通过前后端分离运行的方式来开启热模块重载（HMR）特性：

1. **运行后端服务（端口 8080）**：在您的 IDE 中运行 `KafkaFlowApplication` 或通过终端启动。
2. **运行前端开发服务器（端口 3000）**：
   ```bash
   cd frontend
   npm run dev
   ```
3. 打开浏览器访问 **`http://localhost:3000`**。Vite 开发服务器将自动将所有以 `/api` 开头的请求代理转发至运行在 `8080` 端口的 Spring Boot 后端中。
