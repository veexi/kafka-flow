package com.example.kafkaflow.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class KafkaClientManager {
    private static final Logger log = LoggerFactory.getLogger(KafkaClientManager.class);

    private final AppConfig appConfig;
    private final Map<String, AdminClient> adminClients = new ConcurrentHashMap<>();
    private final Map<String, KafkaProducer<byte[], byte[]>> producers = new ConcurrentHashMap<>();

    public KafkaClientManager(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    /**
     * Get or create a thread-safe AdminClient for the given cluster.
     */
    public AdminClient getAdminClient(String clusterId) {
        return adminClients.computeIfAbsent(clusterId, id -> {
            AppConfig.ClusterSettings cluster = appConfig.getCluster(id)
                    .orElseThrow(() -> new IllegalArgumentException("Cluster not found in config: " + id));
            
            Properties props = buildCommonProperties(cluster);
            log.info("Creating AdminClient for cluster: {} ({})", cluster.name(), id);
            return AdminClient.create(props);
        });
    }

    /**
     * Get or create a thread-safe Producer for the given cluster.
     */
    public KafkaProducer<byte[], byte[]> getProducer(String clusterId) {
        return producers.computeIfAbsent(clusterId, id -> {
            AppConfig.ClusterSettings cluster = appConfig.getCluster(id)
                    .orElseThrow(() -> new IllegalArgumentException("Cluster not found in config: " + id));

            Properties props = buildCommonProperties(cluster);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 1);

            log.info("Creating KafkaProducer for cluster: {} ({})", cluster.name(), id);
            return new KafkaProducer<>(props);
        });
    }

    /**
     * Creates a temporary, non-thread-safe Consumer for short-lived metadata/message inspection.
     * The consumer MUST be closed by the caller in a try-with-resources block.
     */
    public KafkaConsumer<byte[], byte[]> createConsumer(String clusterId, String groupId) {
        AppConfig.ClusterSettings cluster = appConfig.getCluster(clusterId)
                .orElseThrow(() -> new IllegalArgumentException("Cluster not found in config: " + clusterId));

        Properties props = buildCommonProperties(cluster);
        
        // Randomize group ID or use the specified one to avoid partition rebalances during manual inspections
        String resolvedGroupId = (groupId == null || groupId.isEmpty()) 
                ? "kafkaflow-temp-group-" + UUID.randomUUID() 
                : groupId;
                
        props.put(ConsumerConfig.GROUP_ID_CONFIG, resolvedGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(props);
    }

    /**
     * Construct standard configuration properties including SSL and SASL parameters.
     */
    private Properties buildCommonProperties(AppConfig.ClusterSettings cluster) {
        Properties props = new Properties();
        
        // Bootstrap Servers
        String bootstrap = String.join(",", cluster.brokers());
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000); // 10s timeout

        boolean hasSasl = cluster.sasl() != null && cluster.sasl().mechanism() != null;
        boolean hasSsl = cluster.ssl() != null;

        // Security Protocol Setup
        if (hasSasl && hasSsl) {
            props.put("security.protocol", "SASL_SSL");
        } else if (hasSasl) {
            props.put("security.protocol", "SASL_PLAINTEXT");
        } else if (hasSsl) {
            props.put("security.protocol", "SSL");
        }

        // SASL Configuration
        if (hasSasl) {
            String mechanism = cluster.sasl().mechanism().toUpperCase();
            props.put("sasl.mechanism", mechanism);

            if ("PLAIN".equals(mechanism)) {
                String jaas = String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    cluster.sasl().username(), cluster.sasl().password());
                props.put("sasl.jaas.config", jaas);

            } else if (mechanism.startsWith("SCRAM")) {
                String jaas = String.format(
                    "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                    cluster.sasl().username(), cluster.sasl().password());
                props.put("sasl.jaas.config", jaas);

            } else if ("GSSAPI".equals(mechanism)) {
                // Kerberos service name (default: "kafka")
                String serviceName = cluster.sasl().kerberosServiceName() != null
                        && !cluster.sasl().kerberosServiceName().isBlank()
                        ? cluster.sasl().kerberosServiceName() : "kafka";
                props.put("sasl.kerberos.service.name", serviceName);

                // Optional: override krb5.conf path
                if (cluster.sasl().kerberosKrb5Conf() != null
                        && !cluster.sasl().kerberosKrb5Conf().isBlank()) {
                    System.setProperty("java.security.krb5.conf", cluster.sasl().kerberosKrb5Conf());
                    log.info("Using custom krb5.conf: {}", cluster.sasl().kerberosKrb5Conf());
                }

                String authType = cluster.sasl().kerberosAuthType() != null
                        ? cluster.sasl().kerberosAuthType().toUpperCase() : "KEYTAB";
                String principal = cluster.sasl().kerberosPrincipal() != null
                        ? cluster.sasl().kerberosPrincipal() : "";
                final String jaas;

                if ("KEYTAB".equals(authType)) {
                    String keytabPath = cluster.sasl().kerberosKeytabPath();
                    if (keytabPath == null || keytabPath.isBlank()) {
                        throw new IllegalArgumentException("kerberosKeytabPath is required for KEYTAB auth type");
                    }
                    jaas = String.format(
                        "com.sun.security.auth.module.Krb5LoginModule required " +
                        "useKeyTab=true storeKey=true doNotPrompt=true " +
                        "keyTab=\"%s\" principal=\"%s\";",
                        keytabPath, principal);

                } else if ("TICKET_CACHE".equals(authType)) {
                    if (principal.isBlank()) {
                        // Let it automatically pick up default principal from system ticket cache
                        jaas = "com.sun.security.auth.module.Krb5LoginModule required " +
                               "useTicketCache=true doNotPrompt=true;";
                    } else {
                        jaas = String.format(
                            "com.sun.security.auth.module.Krb5LoginModule required " +
                            "useTicketCache=true doNotPrompt=true principal=\"%s\";",
                            principal);
                    }

                } else if ("PASSWORD".equals(authType)) {
                    String krbPassword = cluster.sasl().password() != null ? cluster.sasl().password() : "";
                    jaas = String.format(
                        "com.sun.security.auth.module.Krb5LoginModule required " +
                        "principal=\"%s\" password=\"%s\";",
                        principal, krbPassword);

                } else if ("SYSTEM".equals(authType)) {
                    // Do not inject sasl.jaas.config to fall back to system JVM config -Djava.security.auth.login.config
                    jaas = null;
                    log.info("System level / JVM level JAAS configuration selected. Bypassing sasl.jaas.config generation.");

                } else {
                    throw new IllegalArgumentException("Unsupported Kerberos auth type: " + authType);
                }

                if (jaas != null) {
                    props.put("sasl.jaas.config", jaas);
                    log.info("Configured Kerberos GSSAPI auth: type={}, principal={}", authType, principal);
                } else {
                    log.info("Bypassed programmatically set JAAS config to fall back to JVM defaults.");
                }
            } else {
                throw new IllegalArgumentException("Unsupported SASL mechanism: " + mechanism);
            }
        }

        // SSL Configuration
        if (hasSsl) {
            AppConfig.SslSettings ssl = cluster.ssl();
            if (ssl.truststoreLocation() != null) {
                props.put("ssl.truststore.location", ssl.truststoreLocation());
                props.put("ssl.truststore.password", ssl.truststorePassword());
            }
            if (ssl.keystoreLocation() != null) {
                props.put("ssl.keystore.location", ssl.keystoreLocation());
                props.put("ssl.keystore.password", ssl.keystorePassword());
            }
        }

        return props;
    }

    /**
     * Test connection for a dynamic, unsaved cluster configuration.
     */
    public void testConnection(AppConfig.ClusterSettings cluster) throws Exception {
        Properties props = buildCommonProperties(cluster);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000);
        
        try (AdminClient tempAdmin = AdminClient.create(props)) {
            tempAdmin.describeCluster().clusterId().get(3000, java.util.concurrent.TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Clear caches on application shutdown.
     */
    @PreDestroy
    public synchronized void shutdown() {
        log.info("Closing cached AdminClients and KafkaProducers...");
        
        adminClients.forEach((id, client) -> {
            try {
                client.close();
            } catch (Exception e) {
                log.error("Failed to close AdminClient for cluster: " + id, e);
            }
        });
        adminClients.clear();

        producers.forEach((id, producer) -> {
            try {
                producer.close();
            } catch (Exception e) {
                log.error("Failed to close Producer for cluster: " + id, e);
            }
        });
        producers.clear();
    }

    /**
     * Flush connection cache for a cluster to force reconnection on next call.
     */
    public synchronized void forceReconnect(String clusterId) {
        log.info("Evicting connection cache for cluster: {}", clusterId);
        AdminClient admin = adminClients.remove(clusterId);
        if (admin != null) {
            try { admin.close(); } catch (Exception ignored) {}
        }
        KafkaProducer<byte[], byte[]> prod = producers.remove(clusterId);
        if (prod != null) {
            try { prod.close(); } catch (Exception ignored) {}
        }
    }
}
