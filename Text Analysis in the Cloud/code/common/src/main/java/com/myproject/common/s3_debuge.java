package com.myproject.common;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.UUID;

/**
 * S3 logger:
 *  - Mode A: append to single file (NOT recommended)
 *  - Mode B: folder-based logs (safe, concurrent)
 */
public class s3_debuge {

    private final S3Client s3;
    private final String bucketName;
    private final String logKey;   // file OR folder prefix
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss.SSS");

    private static final int MAX_RETRIES = 5;

    public s3_debuge(S3Client s3, String bucketName, String logKey) {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.logKey = logKey;
    }

    // ----------------------------------------------------------------------
    // MODE B — SAFE OPTION: write each entry as a separate S3 object
    // ----------------------------------------------------------------------
    public void logToFolder(String source, String message) {
        String timestamp = LocalDateTime.now().format(FORMATTER);

        // logKey is treated as a folder prefix
        // example: logs/2025-12-01_22-51-12.123_Manager.txt
        String objectKey = String.format(
                "%s/%s_%s_%s.txt",
                logKey,
                timestamp,
                source,
                UUID.randomUUID()  // ensures uniqueness
        );

        String content = String.format("[%s] [%s] %s\n", timestamp, source, message);

        PutObjectRequest req = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .contentType("text/plain")
                .build();

        s3.putObject(req, RequestBody.fromString(content));

        System.out.printf("Folder log written: %s\n", objectKey);
    }

    // ----------------------------------------------------------------------
    // MODE A — Unsafe: append to single file (old logic)
    // ----------------------------------------------------------------------

    public void createFileIfNotExists() {
        try {
            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(logKey)
                    .build();

            s3.getObject(getRequest, (response, inputStream) -> null);
            System.out.printf("S3 log file already exists: %s\n", logKey);

        } catch (NoSuchKeyException e) {
            System.out.println("S3 log file not found. Creating: " + logKey);
            try {
                PutObjectRequest putRequest = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(logKey)
                        .contentType("text/plain")
                        .build();

                s3.putObject(putRequest, RequestBody.fromString(""));
                System.out.println("Created empty log file: " + logKey);
            } catch (Exception ex) {
                System.err.printf("Failed creating log file %s: %s\n", logKey, ex.getMessage());
                ex.printStackTrace();
            }
        } catch (Exception e) {
            System.err.printf("Error checking existence of %s: %s\n", logKey, e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * (Old mode) Append to one file (NOT recommended — S3 is not atomic)
     */
    public void log(String source, String message) {
        String timestamp = LocalDateTime.now().format(FORMATTER);
        String newEntry = String.format("[%s] [%s] %s\n", timestamp, source, message);

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {

            String existingContent = "";
            AtomicReference<String> etagRef = new AtomicReference<>(null);

            // Read content + ETag
            try {
                GetObjectRequest getRequest = GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(logKey)
                        .build();

                existingContent = s3.getObject(getRequest, (response, inputStream) -> {

                    etagRef.set(response.eTag());

                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                        StringBuilder content = new StringBuilder();
                        String line;
                        while ((line = reader.readLine()) != null) {
                            content.append(line).append('\n');
                        }
                        return content.toString();
                    }
                });

            } catch (NoSuchKeyException e) {
                etagRef.set(null);
                existingContent = "";
                System.out.println("Log file missing during read. Initializing with entry...");
            } catch (Exception e) {
                System.err.printf("ERROR during read attempt %d: %s\n", attempt, e.getMessage());
                if (attempt == MAX_RETRIES - 1) return;
                continue;
            }

            String updatedContent = existingContent + newEntry;

            try {
                PutObjectRequest.Builder putBuilder = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(logKey)
                        .contentType("text/plain");

                // ❌ S3 SDK v2 does NOT support conditional PUT… keeping this for structure only
                String etag = etagRef.get();
                if (etag != null) {
                    // putBuilder.ifMatch(etag);  <-- DOES NOT EXIST IN S3 PUT
                }

                s3.putObject(putBuilder.build(), RequestBody.fromString(updatedContent));

                System.out.printf("Logged to S3 (attempt %d): %s\n", attempt + 1, logKey);
                return;

            } catch (S3Exception e) {
                if (e.statusCode() == 412) {
                    long sleep = 100 + ThreadLocalRandom.current().nextLong(50, 200) * (attempt + 1);
                    System.out.printf("ETag conflict (412). Retry in %dms\n", sleep);

                    try { Thread.sleep(sleep); } catch (InterruptedException ignored) {}
                    continue;
                }

                System.err.printf("Write error at attempt %d: %s\n", attempt, e.getMessage());
                return;

            } catch (Exception e) {
                System.err.printf("Unexpected write error attempt %d: %s\n", attempt, e.getMessage());
                return;
            }
        }
    }
}
