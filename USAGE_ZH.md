# KafkaFlow 核心功能与使用技术手册 🚀

欢迎使用 **KafkaFlow**！本手册旨在帮助开发人员、测试人员及运维团队快速理解 KafkaFlow 的功能设计、业务架构，并掌握在不同环境下的部署与配置技巧。

---

## 目录
1. [项目定位与价值](#1-项目定位与价值)
2. [核心功能特性](#2-核心功能特性)
3. [业务功能操作指南](#3-业务功能操作指南)
   - [集群连接管理与安全认证](#集群连接管理与安全认证)
   - [Topic 主题管理与一键清理](#topic-主题管理与一键清理)
   - [消息浏览器与双层检索机制](#消息浏览器与双层检索机制)
   - [Avro 数据反序列化双模引擎](#avro-数据反序列化双模引擎)
   - [消费者组监控与积压 LAG 计算](#消费者组监控与积压-lag-计算)
4. [高级企业级特性与托管部署](#4-高级企业级特性与托管部署)
   - [Keytab 多集群防冲突物理隔离](#keytab-多集群防冲突物理隔离)
   - [System Default 全局 JVM 级托管](#system-default-全局-jvm-级托管)
   - [生产部署 (Systemd 自启动服务)](#生产部署-systemd-自启动服务)

---

## 1. 项目定位与价值

**KafkaFlow** 是一款现代、轻量、高颜值的 Apache Kafka 网页版可视化客户端。
* **解决的痛点**：传统的 Kafka 桌面端工具（如 Offset Explorer / Kafka Tool）需要每位员工在本地安装，且在配置复杂的 **Kerberos**、**SSL 双向加密**时极其繁琐，证书文件分发存在安全隐患。
* **解决方案**：KafkaFlow 采用 **Single-Executable (单可执行文件)** 架构，将 React 极奢暗黑前端与 Spring Boot 高并发后端编译为**单个 `.jar` 包**。您只需在公司测试环境或堡垒机上部署一次，全员即可通过浏览器安全、直观地访问集群。

---

## 2. 核心功能特性

```
┌────────────────────────────────────────────────────────┐
│                      React 极奢 UI                      │
└───────────────────────────┬────────────────────────────┘
                            │ (REST & WebSocket APIs)
┌───────────────────────────▼────────────────────────────┐
│                    Spring Boot 服务端                    │
├───────────────────────────┬────────────────────────────┤
│   Admin / Producer 缓存   │  UUID 隔离的 Keytab 存储     │
├───────────────────────────┼────────────────────────────┤
│   双模 Avro 编解码引擎     │  双重熔断 Broker 消息扫描    │
└───────────────────────────┴────────────────────────────┘
```

* **安全认证全能王**：动态支持明文、SASL/PLAIN、SASL/SCRAM、双向 SSL 加密以及企业级 Kerberos 认证。
* **极致性能的消息检索**：提供 4 种寻址消费模式（最早、最新、指定 Offset、指定时间戳），首创**前端实时过滤 + 后端深度扫描**的双层检索机制。
* **高内聚无依赖**：生产环境零 Node.js 运行时依赖，开箱即用。

---

## 3. 业务功能操作指南

### 集群连接管理与安全认证

KafkaFlow 提供了全能的安全协议配置页面：

1. **基本连接**：输入集群别名、Bootstrap Servers 地址即可。
2. **连接测试**：保存前点击 **Test Connection**，后端会建立一个 3 秒超时的临时通道进行探活，避免保存错误配置。
3. **SASL 认证**：支持 `PLAIN`、`SCRAM-SHA-256` 及 `SCRAM-SHA-512`，按需输入账号密码。
4. **SSL 加密**：
   * 勾选 **Enable SSL/TLS**，可分别填入 Truststore/Keystore 的服务器绝对路径及密码，用于通道加密与双向身份校验。
5. **Kerberos (SASL/GSSAPI) 认证（王牌功能）**：
   * **Keytab 模式**：点击 Upload 选择本地 `.keytab` 文件，系统将自动上传并生成防冲突路径，填入 Principal 与 krb5.conf 路径（可选）即可。
   * **Ticket Cache 模式**：依赖服务运行主机的 `kinit` 缓存，Principal 可留空。
   * **System Default 模式**：完全托管给 JVM 启动参数（如 `-Djava.security.auth.login.config`），免配置直接连接。

---

### Topic 主题管理与一键清理

提供完整的 Topic 生命周期管控：

* **分区元数据看板**：清晰展示每个 Topic 的 Partition ID、Leader 节点、副本（Replicas）及 ISR（同步副本）列表、分区的 Start/End Offset 和消息总数。
* **新建与删除**：图形化创建 Topic（可指定分区数与副本因子），支持安全级删除。
* **一键 Purge（清理数据）**：
  * *原理*：不采用暴力重建 Topic 的方式（重建可能导致客户端断连或分区分配混乱）。
  * *实现*：利用 `AdminClient.deleteRecords`，将 Topic 各个分区的低水位（Low Watermark）瞬间推送至当前分区的最新高水位（High Watermark），在 Broker 侧实现数据的秒级平滑抹除。

---

### 消息浏览器与双层检索机制

支持海量消息的快速定位与内容筛选：

#### 寻址策略 (Seek Type)
* **Newest / End**：从最新产生的消息开始逆序 poll，非常适合观察实时数据流。
* **Oldest / Start**：从 Topic 最早一条现存消息开始顺序 poll。
* **Specific Offset**：精准跳转到指定分区的指定偏移量处开始消费。
* **Date / Timestamp**：选择日期时间，系统会通过 Kafka 协议将时间转换为时间戳，查找该时间戳对应的 Offset 并开始消费，免去繁琐的换算。

#### 双层检索 (Bilingual Search)
1. **前端即时过滤 (Filter)**：在数据表格上方的 Search 输入框中输入关键字，前端将自适应宽屏平铺展示，并对已加载消息的 Payload 进行瞬时字串匹配与 `...` 省略号裁剪，带来丝滑交互。
2. **服务端深度扫描 (Server Search)**：在 Query Bar 填入 **Server Search Keyword**。后端将在 Broker 侧 poll 消息，并在内存中进行解包，深度匹配 Key、Value 内容、Offset 及 Headers 头信息。
   * *双重熔断保护*：为了防止大词、空词扫描导致拉死集群，后端设定了**扫描上限 30,000 条消息**或**扫描限时 6 秒**的强熔断，一旦触发任何一个阈值，立即返回已扫描到的匹配项，确保服务绝对安全。

---

### Avro 数据反序列化双模引擎

针对 Schema 管理严格的生产环境，KafkaFlow 提供了强大的 Avro 编解码支持：

1. **Schema 注册表动态解析模式**：
   * 配置集群时填入 **Schema Registry** 地址。
   * 消费消息时，`AvroDeserializer` 会自动识别 Confluent 二进制线缆格式的前 5 个字节（`0x00` 幻数 + 4 字节 Schema ID）。
   * 自动请求注册表获取对应的 Schema，并将二进制消息无缝反序列化为易读的格式化 JSON 呈现。
2. **手动 Schema 粘贴解析模式**：
   * 如果没有搭建统一的 Schema 注册表，可勾选 **Provide custom local Avro schema**。
   * 在弹出的文本域中直接粘贴该 Topic 的 `.avsc` 格式 Schema 内容。
   * 系统将直接使用本地的 Schema 编译器在前端/后端完成数据解包，灵活度极高。

---

### 消费者组监控与积压 LAG 计算

直观洞察数据的堆积情况：

* 展示所有活跃的 Consumer Groups 列表及其消费状态。
* 深入到组内，展示每位 Client 成员的 ID、物理主机 IP、分配到的 Partition 详情。
* **LAG 计算**：实时调用 Broker 获取分区的 `LogEndOffset`，并减去当前消费组提交的 `CurrentOffset`，精准抓取每个分区的消息**积压数**，便于排查消费卡顿与性能瓶颈。

---

## 4. 高级企业级特性与托管部署

### Keytab 多集群防冲突物理隔离

在多集群运维中，经常需要上传同名但密钥不同的文件（例如，两个不同的集群都使用名为 `kafka.keytab` 的文件进行认证）。
* **改进前**：直接保存在 `./keytabs/` 下，后上传的文件会把先上传的文件覆盖掉，导致先配置的集群连接断开。
* **改进后**：`KeytabController` 收到上传的 `.keytab` 文件后，会自动生成一个全局唯一的 **UUID 子目录** 并将其存入（例如 `./keytabs/2b591b6c-c9d3-48ee-a63e-f67b5e43a992/kafka.keytab`）。
* 系统将该绝对路径与集群 ID 绑定并保存至 `config.json`，彻底实现物理隔离，多集群共存互不干扰。

---

### System Default 全局 JVM 级托管

在很多大客户或金融级生产部署中，出于安全合规要求，系统的密钥、JAAS 配置文件不允许保存在 Web 应用的数据库中，而是希望完全由系统管理员在服务器底层进行配置。
* **解决方案**：在配置 Kerberos 时，选择 **`System Default (JVM / OS)`** 认证类型。
* **底层机制**：后端 `KafkaClientManager` 将完全**跳过**在代码中注入 `sasl.jaas.config` 属性。
* Kafka 驱动会直接**自动回退**去读取系统环境变量或 JVM 启动时指定的参数：
  ```bash
  -Djava.security.auth.login.config=/etc/security/kafka_client_jaas.conf
  ```
  这实现了 Web 应用与底层秘钥管理的高级解耦。

---

### 生产部署 (Systemd 自启动服务)

在您的 Linux 生产/测试服务器上，建议使用 **Systemd** 进行自启动与后台进程托管：

1. 创建运行目录并拷贝打包好的 `kafka-flow-1.0.0.jar` 和 `config.json`：
   ```bash
   sudo mkdir -p /opt/kafka-flow
   sudo cp target/kafka-flow-1.0.0.jar /opt/kafka-flow/
   sudo cp config.json /opt/kafka-flow/
   ```
2. 编辑服务配置文件 `/etc/systemd/system/kafka-flow.service`：
   ```ini
   [Unit]
   Description=KafkaFlow Visual Web Service
   After=syslog.target network.target

   [Service]
   Type=simple
   User=root
   WorkingDirectory=/opt/kafka-flow
   # 如果需要使用 System Default 级别的全局 JVM Kerberos 认证，在此处追加启动参数：
   # ExecStart=/usr/bin/java -Djava.security.krb5.conf=/etc/krb5.conf -Djava.security.auth.login.config=/opt/kafka-flow/jaas.conf -jar kafka-flow-1.0.0.jar
   ExecStart=/usr/bin/java -jar kafka-flow-1.0.0.jar
   Restart=always
   RestartSec=10
   StandardOutput=syslog
   StandardError=syslog
   SyslogIdentifier=kafka-flow

   [Install]
   WantedBy=multi-user.target
   ```
3. 载入并启动服务：
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable kafka-flow
   sudo systemctl start kafka-flow
   ```
4. 查看服务状态：
   ```bash
   sudo systemctl status kafka-flow
   ```
