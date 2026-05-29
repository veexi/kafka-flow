package com.example.kafkaflow.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;

@RestController
@RequestMapping("/api/keytab")
@CrossOrigin(origins = "*")
public class KeytabController {
    private static final Logger log = LoggerFactory.getLogger(KeytabController.class);
    private static final String KEYTAB_DIR = "./keytabs";

    /**
     * Upload a Kerberos keytab file to the server.
     * Returns the absolute path where it was saved, for use in cluster configuration.
     */
    @PostMapping("/upload")
    public ResponseEntity<Map<String, String>> uploadKeytab(
            @RequestParam("file") MultipartFile file) {

        // Security: only accept .keytab files
        String originalName = file.getOriginalFilename();
        if (originalName == null || !originalName.toLowerCase().endsWith(".keytab")) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "Only .keytab files are accepted."));
        }

        // Security: prevent path traversal
        String safeName = Paths.get(originalName).getFileName().toString();
        if (safeName.isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "Invalid filename."));
        }

        try {
            // Ensure keytabs directory exists
            File dir = new File(KEYTAB_DIR);
            if (!dir.exists()) {
                dir.mkdirs();
            }

            // Create a unique subfolder using UUID to prevent naming collisions
            String uuid = java.util.UUID.randomUUID().toString();
            Path targetPath = Paths.get(KEYTAB_DIR, uuid, safeName).toAbsolutePath().normalize();

            // Security: ensure target is inside our keytabs dir
            Path keytabDirAbs = Paths.get(KEYTAB_DIR).toAbsolutePath().normalize();
            if (!targetPath.startsWith(keytabDirAbs)) {
                return ResponseEntity.badRequest()
                        .body(Map.of("error", "Path traversal attempt detected."));
            }

            // Ensure parent directory (the UUID folder) exists
            File parentDir = targetPath.getParent().toFile();
            if (!parentDir.exists()) {
                parentDir.mkdirs();
            }

            Files.copy(file.getInputStream(), targetPath, StandardCopyOption.REPLACE_EXISTING);
            log.info("Keytab file uploaded to isolated path: {}", targetPath);

            return ResponseEntity.ok(Map.of(
                "path", targetPath.toString(),
                "filename", safeName,
                "size", String.valueOf(file.getSize())
            ));

        } catch (IOException e) {
            log.error("Failed to save keytab file: " + originalName, e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to save file: " + e.getMessage()));
        }
    }

    /**
     * List all uploaded keytab files on the server recursively.
     */
    @GetMapping("/list")
    public ResponseEntity<java.util.List<Map<String, String>>> listKeytabs() {
        File dir = new File(KEYTAB_DIR);
        if (!dir.exists() || !dir.isDirectory()) {
            return ResponseEntity.ok(java.util.Collections.emptyList());
        }

        try (java.util.stream.Stream<Path> stream = Files.walk(Paths.get(KEYTAB_DIR))) {
            java.util.List<Map<String, String>> result = stream
                .filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".keytab"))
                .map(p -> {
                    // Try to extract the UUID or keep a friendly label
                    String filename = p.getFileName().toString();
                    File file = p.toFile();
                    return Map.of(
                        "filename", filename,
                        "path", p.toAbsolutePath().toString(),
                        "size", String.valueOf(file.length())
                    );
                })
                .collect(java.util.stream.Collectors.toList());
            return ResponseEntity.ok(result);
        } catch (IOException e) {
            log.error("Failed to walk keytabs directory", e);
            return ResponseEntity.ok(java.util.Collections.emptyList());
        }
    }
}
