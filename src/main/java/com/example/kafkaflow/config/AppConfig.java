package com.example.kafkaflow.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.*;

@Component
public class AppConfig {
    private static final Logger log = LoggerFactory.getLogger(AppConfig.class);

    @Value("${kafkaflow.config-path:config.json}")
    private String configPath;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private KafkaFlowSettings settings;

    // Config Record Classes (using Java 17 records)
    public record SaslSettings(
        // Used by PLAIN and SCRAM mechanisms
        String mechanism,
        String username,
        String password,
        // Used by GSSAPI (Kerberos) mechanism
        String kerberosAuthType,     // "KEYTAB" | "TICKET_CACHE" | "PASSWORD"
        String kerberosPrincipal,    // e.g. kafka/hostname@REALM
        String kerberosKeytabPath,   // absolute path to the .keytab file on the server
        String kerberosServiceName,  // usually "kafka"
        String kerberosKrb5Conf      // optional path to krb5.conf
    ) {}

    public record SslSettings(
        String truststoreLocation,
        String truststorePassword,
        String keystoreLocation,
        String keystorePassword,
        Boolean skipHostnameVerification
    ) {}

    public record ClusterSettings(
        String id,
        String name,
        List<String> brokers,
        String schemaRegistry,
        SaslSettings sasl,
        SslSettings ssl
    ) {}

    public record KafkaFlowSettings(
        Integer port,
        List<ClusterSettings> clusters
    ) {}

    @PostConstruct
    public void init() {
        loadConfig();
    }

    public synchronized void loadConfig() {
        File file = new File(configPath);
        if (!file.exists()) {
            log.warn("Configuration file not found at: {}. Using default empty configuration.", file.getAbsolutePath());
            this.settings = new KafkaFlowSettings(8080, new ArrayList<>());
            return;
        }

        try {
            log.info("Loading KafkaFlow configuration from: {}", file.getAbsolutePath());
            this.settings = objectMapper.readValue(file, KafkaFlowSettings.class);
            log.info("Successfully loaded {} clusters from config.", this.settings.clusters().size());
        } catch (IOException e) {
            log.error("Failed to parse configuration file at: " + file.getAbsolutePath(), e);
            throw new RuntimeException("Failed to initialize KafkaFlow configuration", e);
        }
    }

    public synchronized void saveConfig(KafkaFlowSettings newSettings) {
        File file = new File(configPath);
        try {
            log.info("Saving KafkaFlow configuration to: {}", file.getAbsolutePath());
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(file, newSettings);
            this.settings = newSettings;
            log.info("Successfully saved configuration to file.");
        } catch (IOException e) {
            log.error("Failed to write configuration file at: " + file.getAbsolutePath(), e);
            throw new RuntimeException("Failed to save KafkaFlow configuration", e);
        }
    }

    public synchronized List<ClusterSettings> getClusters() {
        return settings != null && settings.clusters() != null ? settings.clusters() : Collections.emptyList();
    }

    public synchronized Optional<ClusterSettings> getCluster(String id) {
        return getClusters().stream()
                .filter(c -> c.id().equalsIgnoreCase(id))
                .findFirst();
    }

    public synchronized Integer getPort() {
        return settings != null && settings.port() != null ? settings.port() : 8080;
    }
}
