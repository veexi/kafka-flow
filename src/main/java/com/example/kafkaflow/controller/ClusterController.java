package com.example.kafkaflow.controller;

import com.example.kafkaflow.config.AppConfig;
import com.example.kafkaflow.config.KafkaClientManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.io.File;

@RestController
@RequestMapping("/api/clusters")
@CrossOrigin(origins = "*") // Allow frontend development hot-reloading requests
public class ClusterController {
    private static final Logger log = LoggerFactory.getLogger(ClusterController.class);

    private final AppConfig appConfig;
    private final KafkaClientManager clientManager;

    public ClusterController(AppConfig appConfig, KafkaClientManager clientManager) {
        this.appConfig = appConfig;
        this.clientManager = clientManager;
    }

    // Dynamic Cluster DTO
    public record ClusterDto(
        String id,
        String name,
        List<String> brokers,
        boolean hasSchemaRegistry,
        boolean hasSasl,
        boolean hasSsl,
        String status,
        String error
    ) {}

    @GetMapping
    public List<ClusterDto> listClusters(@RequestParam(value = "checkStatus", defaultValue = "false") boolean checkStatus) {
        List<ClusterDto> list = new ArrayList<>();
        
        for (AppConfig.ClusterSettings cluster : appConfig.getClusters()) {
            String status = "UNKNOWN";
            String errorMsg = null;

            if (checkStatus) {
                try {
                    AdminClient admin = clientManager.getAdminClient(cluster.id());
                    // Perform a quick metadata description to check network availability (1.5s timeout)
                    DescribeClusterResult result = admin.describeCluster();
                    result.clusterId().get(1500, TimeUnit.MILLISECONDS);
                    status = "CONNECTED";
                } catch (Exception e) {
                    status = "DISCONNECTED";
                    errorMsg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                    // Force clean connection cache so it tries again next time
                    clientManager.forceReconnect(cluster.id());
                }
            }

            list.add(new ClusterDto(
                cluster.id(),
                cluster.name(),
                cluster.brokers(),
                cluster.schemaRegistry() != null && !cluster.schemaRegistry().isEmpty(),
                cluster.sasl() != null && cluster.sasl().mechanism() != null,
                cluster.ssl() != null,
                status,
                errorMsg
            ));
        }
        
        return list;
    }

    @PostMapping("/reload")
    public ResponseEntity<Map<String, Object>> reloadConfig() {
        try {
            appConfig.loadConfig();
            
            // Purge all old connection caches
            for (AppConfig.ClusterSettings cluster : appConfig.getClusters()) {
                clientManager.forceReconnect(cluster.id());
            }

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Configuration reloaded successfully. Caches cleared.");
            response.put("clusterCount", appConfig.getClusters().size());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "Reload failed: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    @GetMapping("/{id}/config")
    public ResponseEntity<AppConfig.ClusterSettings> getClusterConfig(@PathVariable("id") String id) {
        Optional<AppConfig.ClusterSettings> config = appConfig.getCluster(id);
        return config.map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).build());
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> saveCluster(@RequestBody AppConfig.ClusterSettings newCluster) {
        try {
            List<AppConfig.ClusterSettings> clusters = new ArrayList<>(appConfig.getClusters());
            
            String id = newCluster.id();
            if (id == null || id.trim().isEmpty()) {
                id = UUID.randomUUID().toString();
            }
            
            String cleanId = id.trim();
            
            // Map SASL settings cleanly
            AppConfig.SaslSettings sasl = null;
            if (newCluster.sasl() != null && newCluster.sasl().mechanism() != null && !newCluster.sasl().mechanism().trim().isEmpty()) {
                sasl = new AppConfig.SaslSettings(
                    newCluster.sasl().mechanism().trim(),
                    newCluster.sasl().username() != null ? newCluster.sasl().username().trim() : null,
                    newCluster.sasl().password() != null ? newCluster.sasl().password().trim() : null,
                    newCluster.sasl().kerberosAuthType(),
                    newCluster.sasl().kerberosPrincipal(),
                    newCluster.sasl().kerberosKeytabPath(),
                    newCluster.sasl().kerberosServiceName(),
                    newCluster.sasl().kerberosKrb5Conf()
                );
            }

            // Map SSL settings cleanly
            AppConfig.SslSettings ssl = null;
            if (newCluster.ssl() != null && newCluster.ssl().truststoreLocation() != null && !newCluster.ssl().truststoreLocation().trim().isEmpty()) {
                ssl = new AppConfig.SslSettings(
                    newCluster.ssl().truststoreLocation().trim(),
                    newCluster.ssl().truststorePassword() != null ? newCluster.ssl().truststorePassword().trim() : "",
                    newCluster.ssl().keystoreLocation() != null ? newCluster.ssl().keystoreLocation().trim() : null,
                    newCluster.ssl().keystorePassword() != null ? newCluster.ssl().keystorePassword().trim() : null,
                    newCluster.ssl().skipHostnameVerification() != null ? newCluster.ssl().skipHostnameVerification() : false
                );
            }

            AppConfig.ClusterSettings clusterToSave = new AppConfig.ClusterSettings(
                cleanId,
                newCluster.name().trim(),
                newCluster.brokers(),
                newCluster.schemaRegistry() != null && !newCluster.schemaRegistry().trim().isEmpty() ? newCluster.schemaRegistry().trim() : null,
                sasl,
                ssl
            );

            // Remove existing with same ID if present, then add
            clusters.removeIf(c -> c.id().equalsIgnoreCase(cleanId));
            clusters.add(clusterToSave);

            AppConfig.KafkaFlowSettings newSettings = new AppConfig.KafkaFlowSettings(
                appConfig.getPort(),
                clusters
            );

            appConfig.saveConfig(newSettings);
            clientManager.forceReconnect(cleanId); // Force reconnect for this cluster

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Cluster config saved successfully.");
            response.put("clusterId", cleanId);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "Failed to save cluster: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, Object>> deleteCluster(@PathVariable("id") String id) {
        try {
            List<AppConfig.ClusterSettings> clusters = new ArrayList<>(appConfig.getClusters());
            boolean removed = clusters.removeIf(c -> c.id().equalsIgnoreCase(id));
            
            if (!removed) {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "Cluster not found.");
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

            AppConfig.KafkaFlowSettings newSettings = new AppConfig.KafkaFlowSettings(
                appConfig.getPort(),
                clusters
            );

            appConfig.saveConfig(newSettings);
            clientManager.forceReconnect(id); // Clean up caches

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Cluster config deleted successfully.");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "Failed to delete cluster: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    @PostMapping("/test-connection")
    public ResponseEntity<Map<String, Object>> testClusterConnection(@RequestBody AppConfig.ClusterSettings cluster) {
        try {
            AppConfig.SaslSettings sasl = null;
            if (cluster.sasl() != null && cluster.sasl().mechanism() != null && !cluster.sasl().mechanism().trim().isEmpty()) {
                sasl = new AppConfig.SaslSettings(
                    cluster.sasl().mechanism().trim(),
                    cluster.sasl().username() != null ? cluster.sasl().username().trim() : null,
                    cluster.sasl().password() != null ? cluster.sasl().password().trim() : null,
                    cluster.sasl().kerberosAuthType(),
                    cluster.sasl().kerberosPrincipal(),
                    cluster.sasl().kerberosKeytabPath(),
                    cluster.sasl().kerberosServiceName(),
                    cluster.sasl().kerberosKrb5Conf()
                );
            }

            AppConfig.SslSettings ssl = null;
            if (cluster.ssl() != null && cluster.ssl().truststoreLocation() != null && !cluster.ssl().truststoreLocation().trim().isEmpty()) {
                ssl = new AppConfig.SslSettings(
                    cluster.ssl().truststoreLocation().trim(),
                    cluster.ssl().truststorePassword() != null ? cluster.ssl().truststorePassword().trim() : "",
                    cluster.ssl().keystoreLocation() != null ? cluster.ssl().keystoreLocation().trim() : null,
                    cluster.ssl().keystorePassword() != null ? cluster.ssl().keystorePassword().trim() : null,
                    cluster.ssl().skipHostnameVerification() != null ? cluster.ssl().skipHostnameVerification() : false
                );
            }

            AppConfig.ClusterSettings clusterToTest = new AppConfig.ClusterSettings(
                "temp-test-connection",
                cluster.name() != null ? cluster.name().trim() : "Temp Test Cluster",
                cluster.brokers(),
                cluster.schemaRegistry() != null && !cluster.schemaRegistry().trim().isEmpty() ? cluster.schemaRegistry().trim() : null,
                sasl,
                ssl
            );

            clientManager.testConnection(clusterToTest);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Connection successful! Broker cluster responded successfully.");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Throwable cause = e;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "Connection failed: " + cause.getMessage());
            return ResponseEntity.ok(response);
        }
    }

    @PostMapping("/upload-ssl-file")
    public ResponseEntity<Map<String, Object>> uploadSslFile(@RequestParam("file") org.springframework.web.multipart.MultipartFile file) {
        try {
            if (file.isEmpty()) {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "File is empty.");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }

            File certsDir = new File("ssl-certs");
            if (!certsDir.exists()) {
                certsDir.mkdirs();
            }

            String originalName = file.getOriginalFilename() != null ? file.getOriginalFilename() : "cert.jks";
            String cleanName = originalName.replaceAll("[^a-zA-Z0-9._-]", "_");
            String savedName = UUID.randomUUID().toString().substring(0, 8) + "_" + cleanName;
            File destinationFile = new File(certsDir, savedName);

            file.transferTo(destinationFile.getAbsoluteFile());

            log.info("Saved uploaded SSL file to: {}", destinationFile.getAbsolutePath());

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "File uploaded successfully!");
            response.put("serverFilePath", destinationFile.getAbsolutePath());
            response.put("fileName", originalName);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "Failed to upload file: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    // REST Exception Handler for all controllers
    @RestControllerAdvice
    public static class GlobalExceptionHandler {
        private static final Logger handlerLog = LoggerFactory.getLogger(GlobalExceptionHandler.class);

        @ExceptionHandler(IllegalArgumentException.class)
        public ResponseEntity<Map<String, String>> handleIllegalArgument(IllegalArgumentException e) {
            Map<String, String> error = new HashMap<>();
            error.put("error", "Bad Request");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
        }

        @ExceptionHandler(Exception.class)
        public ResponseEntity<Map<String, String>> handleGeneralException(Exception e) {
            handlerLog.error("Unhandled API exception", e);
            Map<String, String> error = new HashMap<>();
            error.put("error", "Internal Server Error");
            
            // Get root cause if present
            Throwable cause = e;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            error.put("message", cause.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }
}
