package com.abelini_s3_migrate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class ShopifyService {
    private static final Logger logger = LoggerFactory.getLogger(ShopifyService.class);
    private final RestTemplate restTemplate = new RestTemplate();
    private final Tika tika = new Tika();
    private final ObjectMapper objectMapper;

    @Value("${shopify_store}")
    private String shopifyStore;

    @Value("${shopify_access_token}")
    private String accessToken;

    private final String SHOPIFY_GRAPHQL_URL = shopifyStore + "/admin/api/2025-01/graphql.json";
    private final String SHOPIFY_ACCESS_TOKEN = accessToken;

    public ShopifyService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    private List<String> readCSV(String filePath) throws IOException, CsvException {
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            List<String[]> records = reader.readAll();
            return records.stream().skip(1) // Skip header row
                    .map(row -> row[0])
                    .collect(Collectors.toList());
        }
    }

    private static final int MAX_CONCURRENT_BATCHES = 10;
    private static final Semaphore semaphore = new Semaphore(MAX_CONCURRENT_BATCHES);
    private final AtomicInteger totalProcessed = new AtomicInteger(0);
    private static final int API_COST_PER_CALL = 40;
    private static final int MAX_POINTS = 20000;
    private static final int RECOVERY_RATE = 1000;
    private static final int SAFE_THRESHOLD = 1000;
    private static final AtomicInteger remainingPoints = new AtomicInteger(MAX_POINTS);

    @Async
    public void uploadImagesToShopify(String csvFilePath) throws IOException, CsvException {
        logger.info("Starting bulk upload to Shopify... started at :: {}", ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z")));

        List<String> imageUrls = readCSV(csvFilePath);
        logger.info("Total URLs count: {}", imageUrls.size());

        int batchSize = 100;
        int totalBatches = (int) Math.ceil((double) imageUrls.size() / batchSize);

        ExecutorService executorService = Executors.newFixedThreadPool(MAX_CONCURRENT_BATCHES);

        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < imageUrls.size(); i += batchSize) {
            final int batchNumber = (i / batchSize) + 1;
            final List<String> batch = imageUrls.subList(i, Math.min(i + batchSize, imageUrls.size()));

            futures.add(executorService.submit(() -> {
                try {
                    semaphore.acquire();
                    regulateApiRate();
                    logger.info("Starting batch {} of {} with {} images...", batchNumber, totalBatches, batch.size());
                    remainingPoints.addAndGet(-API_COST_PER_CALL);
                    int count = registerBatchInShopify(batch);

                    int processed = totalProcessed.addAndGet(count);
                    logger.info("Batch {} completed. Total processed so far: {}/{}", batchNumber, processed, imageUrls.size());
                } catch (Exception e) {
                    logger.error("Error uploading batch {}: {}", batchNumber, e.getMessage(), e);
                } finally {
                    semaphore.release();
                }
            }));
        }

        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Batch execution interrupted: {}", e.getMessage(), e);
            }
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.MINUTES)) {
                logger.warn("Executor did not terminate in the specified time.");
                executorService.shutdownNow();
                if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                    logger.error("Executor did not terminate after forced shutdown.");
                }
            } else {
                logger.info("All batches completed successfully within 5 minutes.");
            }
        } catch (InterruptedException e) {
            logger.error("Shutdown interrupted: {}", e.getMessage(), e);
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("Bulk upload completed. Total images processed: {}, ended at :: {}", totalProcessed.get(),ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z")));
    }

    private void regulateApiRate() {
        if (remainingPoints.get() < SAFE_THRESHOLD) {
            int waitTime = Math.min(5, (MAX_POINTS - remainingPoints.get()) / RECOVERY_RATE);
            logger.info("Low API points (" + remainingPoints.get() + "), pausing for " + waitTime + " seconds to recover.");
            try {
                TimeUnit.SECONDS.sleep(waitTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            remainingPoints.addAndGet(waitTime * RECOVERY_RATE);  // Thread-safe increment
        }
    }

    public int registerBatchInShopify(List<String> fileUrls) {
        List<Map<String, String>> filesList = new ArrayList<>();
        for (String fileUrl : fileUrls) {
            String encodedUrl = encodeUrl(fileUrl);
            String fileName = generateShopifyFilePath(fileUrl);
            String contentType = detectShopifyContentType(fileUrl);
            if (notSupportedFileType(fileUrl)) continue;
//            logger.info("contentType ::: {}", contentType);
            Map<String, String> fileEntry = new HashMap<>();
            fileEntry.put("originalSource", encodedUrl);
            fileEntry.put("filename", fileName);
            fileEntry.put("alt", fileName);
            fileEntry.put("contentType", contentType);
            filesList.add(fileEntry);
        }

        if (filesList.isEmpty()) {
            logger.warn("No supported files in this batch.");
            return 0;
        }

        String query = """
                mutation fileCreate($files: [FileCreateInput!]!) {
                    fileCreate(files: $files) {
                        files {
                            id
                            fileStatus
                            alt
                            createdAt
                        }
                        userErrors {
                            field
                            message
                        }
                    }
                }
                """;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> variablesMap = new HashMap<>();
            variablesMap.put("files", filesList);
            String variables = objectMapper.writeValueAsString(variablesMap);
            logger.info("Uploading batch of {} files to Shopify", filesList.size());
            String response = sendGraphQLRequest(query, variables);
            logger.info("Shopify Response: {}", response);
            if (response.contains("\"userErrors\":[")) {
                if (!response.contains("\"userErrors\":[]")) {
                    logger.error("Error uploading batch: {}", response);
                    return 0;
                }
            }
            logger.info("Batch uploaded successfully.");
            return filesList.size();
        } catch (Exception e) {
            logger.error("Error in batch upload: {}", e.getMessage(), e);
            return 0;
        }
    }

    private static final Set<String> SUPPORTED_IMAGE_MIME_TYPES = Set.of(
            "image/png", "image/jpeg", "image/gif", "image/jpg", "image/webp", "image/svg+xml",
            "image/avif", "video/mp4"
    );

    private boolean notSupportedFileType(String fileUrl) {
        String mimeType = detectMimeType(fileUrl);
        return !SUPPORTED_IMAGE_MIME_TYPES.contains(mimeType);
    }

    private String sendGraphQLRequest(String query, String variables) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("X-Shopify-Access-Token", accessToken.trim());
            headers.set("Content-Type", "application/json");
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("query", query);
            requestBody.put("variables", new ObjectMapper().readValue(variables, Map.class));
            String requestJson = new ObjectMapper().writeValueAsString(requestBody);
            HttpEntity<String> request = new HttpEntity<>(requestJson, headers);
            ResponseEntity<String> response = restTemplate.postForEntity(
                    shopifyStore + "/admin/api/2025-01/graphql.json",
                    request,
                    String.class
            );
            return response.getBody();
        } catch (Exception e) {
            logger.error("Error sending GraphQL request: {}", e.getMessage(), e);
            return null;
        }
    }

    private String encodeUrl(String url) {
        try {
            return URLEncoder.encode(url, StandardCharsets.UTF_8)
                    .replace("+", "%20")
                    .replace("%2F", "/")
                    .replace("%3A", ":");
        } catch (Exception e) {
            logger.error("Error encoding URL: {}", e.getMessage());
            return url;
        }
    }

    private String generateShopifyFilePath(String fileUrl) {
        String relativePath = fileUrl.substring(fileUrl.indexOf(".com/") + 5).replace("/", "_");
        String fileName = Pattern.compile("\\s+").matcher(relativePath).replaceAll("_");
//        logger.info("file name ::: {}", fileName);
        return fileName;
    }

    private String detectMimeType(String filename) {
        try {
            return tika.detect(filename);
        } catch (Exception e) {
            logger.warn("Could not detect MIME type for {}. Defaulting to image/jpeg", filename);
            return "image/jpeg";
        }
    }
    private final Set<String> imageMimeTypes = Set.of(
            "image/png", "image/jpeg", "image/gif", "image/jpg", "image/webp", "image/svg+xml"
    );

    private String detectShopifyContentType(String fileUrl) {
        String mimeType = detectMimeType(fileUrl);
        return imageMimeTypes.contains(mimeType) ? "IMAGE" : "FILE";
    }

    public String uploadFileToShopify(String s3Url) throws IOException {
        logger.info("s3 url ::: {}", s3Url);
        String customFileName = generateShopifyFilePath(s3Url);
//        s3Url = encodeUrl(s3Url);
        // 1. Download the file from S3
        byte[] fileBytes = downloadFileFromS3(s3Url);
        if (fileBytes == null) {
            return "Failed to download file from S3";
        }

        // 2. Detect MIME type and determine file type for Shopify
        String mimeType = Files.probeContentType(new File(customFileName).toPath());
        if (mimeType == null) mimeType = "application/octet-stream"; // Default fallback

        String contentType = getShopifyContentType(mimeType);
        logger.info("content type ::: {}", contentType);

        // 3. Request a presigned URL from Shopify
        String uploadUrl = getPresignedUrl(customFileName, mimeType, contentType);
        if (uploadUrl != null) {
            // 4. Upload file to Shopify's presigned URL
            uploadFileToPresignedUrl(uploadUrl, fileBytes, mimeType);

            // 5. Register the uploaded file in Shopify
            return registerFileInShopify(uploadUrl, customFileName, contentType);
        }

        return "Failed to get upload URL";
    }

    private byte[] downloadFileFromS3(String s3Url) {
        try {
            // Validate the URL
            if (s3Url == null || !s3Url.startsWith("http")) {
                throw new IllegalArgumentException("Invalid S3 URL: " + s3Url);
            }

            System.out.println("Downloading file from: " + s3Url); // Debugging

            // Open connection
            URL url = new URL(s3Url);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000); // 5 seconds timeout
            connection.setReadTimeout(5000);

            // Check response code
            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new IOException("Failed to download file. HTTP Response Code: " + responseCode);
            }

            // Read data into byte array
            InputStream inputStream = connection.getInputStream();
            byte[] fileBytes = inputStream.readAllBytes();

            // Close resources
            inputStream.close();
            connection.disconnect();

            logger.info("file downloaded");
            return fileBytes;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private String getPresignedUrl(String fileName, String mimeType, String contentType) {
        logger.info("Getting presigned URL for file: " + fileName + " with MIME type: " + mimeType);

        // Correct GraphQL Query Formatting
        String query = "{ \"query\": \"mutation stagedUploadsCreate($input: [StagedUploadInput!]!) { " +
                "stagedUploadsCreate(input: $input) { stagedTargets { url parameters { name value } } } } }\", " +
                "\"variables\": { \"input\": [{ \"filename\": \"" + fileName + "\", " +
                "\"mimeType\": \"" + mimeType + "\", " + "\"resource\": \"" + contentType + "\" }] } }";

        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Shopify-Access-Token", accessToken);
        headers.set("Content-Type", "application/json");

        HttpEntity<String> entity = new HttpEntity<>(query, headers);

        String shopifyUrl = shopifyStore + "/admin/api/2025-01/graphql.json"; // Corrected API version

        ResponseEntity<String> response = restTemplate.exchange(shopifyUrl, HttpMethod.POST, entity, String.class);

        try {
            logger.info("Shopify Response: " + response.getBody());

            JsonNode root = objectMapper.readTree(response.getBody());
            JsonNode urlNode = root.path("data").path("stagedUploadsCreate").path("stagedTargets").get(0).path("url");

            if (urlNode.isMissingNode() || urlNode.asText().isEmpty()) {
                throw new IllegalArgumentException("Invalid presigned URL received from Shopify.");
            }

            String presignedUrl = urlNode.asText().trim();
            logger.info("Received presigned URL: " + presignedUrl);

            return presignedUrl;
        } catch (Exception e) {
            logger.error("Error while getting presigned URL from Shopify", e);
            return null;
        }
    }

    private void uploadFileToPresignedUrl(String uploadUrl, byte[] fileBytes, String mimeType) {
        logger.info("calling presigned url");
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.parseMediaType(mimeType));

        HttpEntity<byte[]> entity = new HttpEntity<>(fileBytes, headers);
        restTemplate.exchange(uploadUrl, HttpMethod.PUT, entity, String.class);
        logger.info("file uploaded");
    }

    private String registerFileInShopify(String fileUrl, String customFileName, String contentType) {
        String query = "{ \"query\": \"mutation fileCreate($files: [FileCreateInput!]!) " +
                "{ fileCreate(files: $files) { files { id url alt } userErrors { field message } } } }\", " +
                "\"variables\": { \"files\": [ { \"originalSource\": \\\"" + fileUrl + "\\\", \"contentType\": " + contentType + ", \"alt\": \\\"" + customFileName + "\\\" } ] } }";

        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Shopify-Access-Token", accessToken);
        headers.set("Content-Type", "application/json");

        String shopifyUrl = shopifyStore + "/admin/api/2025-01/graphql.json";
        HttpEntity<String> entity = new HttpEntity<>(query, headers);
        ResponseEntity<String> response = restTemplate.exchange(shopifyUrl, HttpMethod.POST, entity, String.class);

        try {
            JsonNode root = objectMapper.readTree(response.getBody());
            return root.path("data").path("fileCreate").path("files").get(0).path("url").asText();
        } catch (Exception e) {
            e.printStackTrace();
            return "Error registering file";
        }
    }

    private String getShopifyContentType(String mimeType) {
        if (mimeType == null) return "FILE"; // Default to generic file if unknown

        if (mimeType.startsWith("image/")) {
            return "IMAGE";
        } else if (mimeType.startsWith("video/")) {
            return "VIDEO";
        } else {
            return "FILE"; // PDFs, text files, etc.
        }
    }
}
