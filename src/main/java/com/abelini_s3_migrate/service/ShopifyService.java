package com.abelini_s3_migrate.service;

import com.abelini_s3_migrate.extra.SkuStats;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.tika.Tika;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
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
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.abelini_s3_migrate.service.ProductMigrationService.API_COST_PER_CALL;
import static com.abelini_s3_migrate.service.ProductMigrationService.remainingPoints;

@Service
public class ShopifyService {
    private static final Logger logger = LoggerFactory.getLogger(ShopifyService.class);
    private final RestTemplate restTemplate = new RestTemplate();
    private final Tika tika = new Tika();
    private final ObjectMapper objectMapper;
    private final ProductMigrationService productMigrationService;

    @Value("${shopify_store}")
    private String shopifyStore;

    @Value("${shopify_access_token_2}")
    private String accessToken;

//    private final String SHOPIFY_GRAPHQL_URL = shopifyStore + "/admin/api/2025-01/graphql.json";
//    private final String SHOPIFY_ACCESS_TOKEN = accessToken;

    private List<String> readCSV(String filePath) throws IOException, CsvException {
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            List<String[]> records = reader.readAll();
            return records.stream().skip(1) // Skip header row
                    .map(row -> row[0])
                    .collect(Collectors.toList());
        }
    }

    private static final int MAX_CONCURRENT_BATCHES = 25;
    private static final Semaphore semaphore = new Semaphore(MAX_CONCURRENT_BATCHES);
    private final AtomicInteger totalProcessed = new AtomicInteger(0);
    //    private static final int API_COST_PER_CALL = 40;
//    private static final int MAX_POINTS = 20000;
//    private static final int RECOVERY_RATE = 1000;
//    private static final int SAFE_THRESHOLD = 2000;
//    private static final AtomicInteger remainingPoints = new AtomicInteger(MAX_POINTS);

//    private final ScheduledExecutorService creditRecoveryScheduler = Executors.newScheduledThreadPool(1);

    public ShopifyService(ObjectMapper objectMapper, ProductMigrationService productMigrationService) {
        this.objectMapper = objectMapper;
        this.productMigrationService = productMigrationService;
//        creditRecoveryScheduler.scheduleAtFixedRate(() -> {
//            int currentPoints = remainingPoints.get();
//            if (currentPoints < MAX_POINTS) {
//                int newPoints = Math.min(RECOVERY_RATE, MAX_POINTS - currentPoints);
//                remainingPoints.addAndGet(newPoints);
//                logger.debug("Recovered {} API points. Current points: {}", newPoints, remainingPoints.get());
//            }
//        }, 1, 1, TimeUnit.SECONDS);
    }

    private final AtomicInteger totalUrlsP = new AtomicInteger(0);
    private final AtomicInteger totalBatchesP = new AtomicInteger(0);
    private final AtomicInteger batchesProcessedP = new AtomicInteger(0);
    private final AtomicInteger batchesSucceededP = new AtomicInteger(0);
    private final AtomicInteger batchesFailedP = new AtomicInteger(0);
    private final AtomicInteger urlsSucceededP = new AtomicInteger(0);
    private final AtomicInteger urlsFailedP = new AtomicInteger(0);
    List<String> failedBatchesP = Collections.synchronizedList(new ArrayList<>());

    public String printSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n======= BULK IMAGE UPLOAD SUMMARY =======\n");
        sb.append(String.format("Total URLs Count         : %d%n", totalUrlsP.get()));
        sb.append(String.format("Total Batches Count      : %d%n", totalBatchesP.get()));
        sb.append(String.format("Batches Processed Count  : %d%n", batchesProcessedP.get()));
        sb.append(String.format("Batches Succeeded Count  : %d%n", batchesSucceededP.get()));
        sb.append(String.format("Batches Failed Count     : %d%n", batchesFailedP.get()));
        sb.append(String.format("Batches Failed No. List  : %s%n", failedBatchesP));
        sb.append(String.format("URLs Success Count       : %d%n", urlsSucceededP.get()));
        sb.append(String.format("URLs Failed Count        : %d%n", urlsFailedP.get()));
        sb.append("===================================\n");
        System.out.print(sb);
        return sb.toString();
    }

    @Async
    public void uploadImagesToShopify(String csvFilePath, Set<Integer> failedBatch, boolean isFailed) throws IOException, CsvException {
        logger.info("Starting bulk upload to Shopify... started at :: {}", ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z")));

        List<String> imageUrls = readCSV(csvFilePath);

        int batchSize = 50;
        int total = isFailed ? failedBatch.size() * batchSize : imageUrls.size();
        logger.info("Total URLs count: {}", total);
        totalUrlsP.set(total);
        int totalBatches = (int) Math.ceil((double) total / batchSize);
        totalBatchesP.set(totalBatches);

        ExecutorService executorService = Executors.newFixedThreadPool(MAX_CONCURRENT_BATCHES);

        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < imageUrls.size(); i += batchSize) {
            final int batchNumber = (i / batchSize) + 1;
            if (isFailed && !failedBatch.contains(batchNumber)) continue;
            final List<String> batch = imageUrls.subList(i, Math.min(i + batchSize, imageUrls.size()));
            logger.info("Starting batch {} of {} with {} images...", batchNumber, totalBatches, batch.size());
            futures.add(executorService.submit(() -> {
                try {
                    semaphore.acquire();
                    productMigrationService.regulateApiRate();
                    logger.info("Starting batch {} of {} with {} images...", batchNumber, totalBatches, batch.size());
                    remainingPoints.addAndGet(-API_COST_PER_CALL);
                    batchesProcessedP.incrementAndGet();
                    int count = registerBatchInShopify(batch);
                    if (count == 0) {
                        batchesFailedP.incrementAndGet();
                        urlsFailedP.addAndGet(batch.size());
                        failedBatchesP.add(String.valueOf(batchNumber));
                    } else {
                        batchesSucceededP.incrementAndGet();
                        urlsSucceededP.addAndGet(batch.size());
                    }

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
//            try {
//                regulateApiRate();
//                remainingPoints.addAndGet(-API_COST_PER_CALL);
//                int count = registerBatchInShopify(batch);
//                totalProcessed.addAndGet(count);
//                logger.info("Batch {} completed. Total processed so far: {}/{}", batchNumber, totalProcessed.get(), imageUrls.size());
//            } catch (Exception e) {
//                logger.error("Error uploading batch {}: {}", batchNumber, e.getMessage(), e);
//            }
//
//            if (i + batchSize < imageUrls.size()) {
//                logger.info("Waiting for 1 second before next batch...");
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    Thread.currentThread().interrupt();
//                }
//            }
//        }
        logger.info("Bulk upload completed. Total images processed: {}, ended at :: {}", totalProcessed.get(), ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z")));
    }

//    private void regulateApiRate() {
//        int maxWaitTime = 10; // Maximum wait time in seconds
//        int waitTime = 0;
//
//        while (remainingPoints.get() < SAFE_THRESHOLD) {
//            if (waitTime >= maxWaitTime) {
//                logger.warn("API points still low after waiting {} seconds. Continuing anyway.", maxWaitTime);
//                break;
//            }
//            logger.info("Low API points ({}), pausing until recovery...", remainingPoints.get());
//            try {
//                Thread.sleep(1000); // Wait 1 second for recovery
//                waitTime++;
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//                break;
//            }
//        }
//    }

    public int registerBatchInShopify(List<String> fileUrls) {
        List<Map<String, String>> filesList = new ArrayList<>();
        for (String fileUrl : fileUrls) {
            if (notSupportedFileType(fileUrl)) continue;
            String encodedUrl = encodeUrl(fileUrl);
            String fileName = generateShopifyFilePath(fileUrl);
            String contentType = detectShopifyContentType(fileUrl);
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
            "image/png", "image/jpeg", "image/gif", "image/jpg", "image/webp", "image/svg+xml"
            , "image/avif", "video/mp4"
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


    private final AtomicInteger totalSkus = new AtomicInteger();
    private final AtomicInteger skusProcessed = new AtomicInteger();
    private final AtomicInteger skusSucceeded = new AtomicInteger();
    private final AtomicInteger skusFailed = new AtomicInteger();

    // Per-SKU Stats Map
    private final Map<String, SkuStats> skuStatsMap = new ConcurrentHashMap<>();

    @Async
    public void updateImagesBySku(List<String> skus, String csvFilePath) {
        String start = ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z"));
        logger.info("Starting updateImagesBySku Shopify... started at :: {}", start);
        try {
            List<String> imageUrls = readCSV(csvFilePath);
            totalSkus.set(skus.size());
            Map<String, String> s3FileNameToUrlMap = imageUrls.stream()
                    .collect(Collectors.toMap(
                            this::generateShopifyFilePath,
                            Function.identity(),
                            (existing, replacement) -> replacement,
                            ConcurrentHashMap::new // <--- thread-safe map
                    ));

            int MAX_CONCURRENT_BATCHES = 10;
            Semaphore semaphore = new Semaphore(MAX_CONCURRENT_BATCHES);
            ExecutorService executorService = Executors.newFixedThreadPool(MAX_CONCURRENT_BATCHES);

            List<Future<?>> futures = new ArrayList<>();

            for (String sku : skus) {
                futures.add(executorService.submit(() -> {
                    try {
                        semaphore.acquire();
                        processSkuBatch(sku, s3FileNameToUrlMap);
                    } catch (Exception e) {
                        logger.error("Error uploading {}", e.getMessage(), e);
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
        } catch (Exception e) {
            logger.error("Error while updating images by SKU", e);
        }

        String end = ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z"));
        logger.info("Ended updateImagesBySku Shopify... started at :: {} and ended at :: {}", start, end);
    }

    private void processSkuBatch(String sku, Map<String, String> s3FileNameToUrlMap) {
        try {
            SkuStats stats = new SkuStats();
            skuStatsMap.put(sku, stats);
            Map<String, String> shopifyFileNameToIdMap = new HashMap<>();
            String cursor = null;
            boolean hasNextPage = false;

            do {
                String query = buildShopifyFileQuery(sku, cursor);
                productMigrationService.regulateApiRate();
                remainingPoints.addAndGet(-API_COST_PER_CALL);
                JSONObject response = executeGraphQLQuery(query);

                if (!response.has("data") || response.isNull("data")) {
                    logger.warn("Shopify response empty, continuing...");
                    continue;
                }

                JSONObject filesObject = response.getJSONObject("data").getJSONObject("files");
                JSONArray edges = filesObject.getJSONArray("edges");
                hasNextPage = filesObject.getJSONObject("pageInfo").getBoolean("hasNextPage");
                cursor = filesObject.getJSONObject("pageInfo").optString("endCursor", null);

                for (int i = 0; i < edges.length(); i++) {
                    JSONObject node = edges.getJSONObject(i).getJSONObject("node");

                    String fileId = node.optString("id");
                    String typename = node.optString("__typename");

                    String fileUrl = null;
                    if ("GenericFile".equals(typename)) {
                        fileUrl = node.optString("url", null);
                    } else if ("MediaImage".equals(typename)) {
                        JSONObject imageObj = node.optJSONObject("image");
                        if (imageObj != null) {
                            fileUrl = imageObj.optString("url", null);
                        }
                    }

                    String cleanFileName = extractCleanFilename(fileUrl);
                    if (!cleanFileName.isBlank() && fileId != null) {
                        shopifyFileNameToIdMap.put(cleanFileName, fileId);
                    }
                }

                logger.info("Fetched " + shopifyFileNameToIdMap.size() + " entries for SKU: " + sku);

            } while (hasNextPage);

            logger.info("Total entries fetched: " + shopifyFileNameToIdMap.size());

            Map<String, String> updateMap = new HashMap<>();

            List<String> createList = new ArrayList<>();

            stats.getImagesFetched().addAndGet(shopifyFileNameToIdMap.size());

            if (!shopifyFileNameToIdMap.isEmpty()) {
                for (Map.Entry<String, String> s3Entry : s3FileNameToUrlMap.entrySet()) {
                    String fileName = s3Entry.getKey();
                    String s3Url = s3Entry.getValue();

                    if (!fileName.toLowerCase().contains(sku.toLowerCase())) {
                        continue; // skip unrelated files
                    }

                    if (shopifyFileNameToIdMap.containsKey(fileName)) {
                        // File exists – schedule for update
                        String shopifyId = shopifyFileNameToIdMap.get(fileName);
                        updateMap.put(shopifyId, s3Url);
                    } else {
                        // File doesn't exist – schedule for creation
                        createList.add(s3Url);
                    }
                }

                stats.getImagesToUpdate().set(updateMap.size());
                stats.getImagesToCreate().set(createList.size());

                if (!updateMap.isEmpty()) {
                    updateImagesAsync(updateMap, stats);
                }
                if (!createList.isEmpty()) {
                    createImagesAsync(createList, stats);
                }

            } else {
                logger.info("No files found for SKU: " + sku);
            }
            skusSucceeded.incrementAndGet();
        } catch (Exception e) {
            logger.error("Error processing SKU: {}", sku, e);
            skusFailed.incrementAndGet();
        } finally {
            skusProcessed.incrementAndGet();
        }
    }

    private void updateImagesAsync(Map<String, String> updateMap, SkuStats stats) {
        int MAX_CONCURRENT_BATCHES = 10;
        Semaphore semaphore = new Semaphore(MAX_CONCURRENT_BATCHES);
        ExecutorService executorService = Executors.newFixedThreadPool(MAX_CONCURRENT_BATCHES);

        int batchSize = 250;
        int total = updateMap.size();
        logger.info("Total URLs count: {}", total);
        int totalBatches = (int) Math.ceil((double) total / batchSize);
        stats.getUpdateTotalBatch().addAndGet(totalBatches);

        List<Future<?>> futures = new ArrayList<>();
        List<Map<String, String>> allBatches = splitMapIntoBatches(updateMap, batchSize, totalBatches, total);

        for (int bn = 0; bn < allBatches.size(); bn++) {
            final int batchNumber = bn + 1;
            final Map<String, String> batch = allBatches.get(bn);
            logger.info("Starting batch {} of {} with {} images...", batchNumber, totalBatches, total);
            futures.add(executorService.submit(() -> {
                try {
                    semaphore.acquire();
                    productMigrationService.regulateApiRate();
                    logger.info("Starting batch {} of {} with {} images...", batchNumber, totalBatches, total);
                    remainingPoints.addAndGet(-API_COST_PER_CALL);
                    int count = updateBatchInShopify(batch);
                    if (count == 0) {
                        batch.forEach((id, url) -> stats.getFailedUpdateMap().put(id, url));
                        stats.getUpdateFailed().addAndGet(batch.size());
                        stats.getUpdateBatchFailed().incrementAndGet();
                        stats.getUpdateFailedBatchList().add(String.valueOf(batchNumber));
                    } else {
                        stats.getUpdateBatchSuccess().incrementAndGet();
                        stats.getUpdateSuccess().addAndGet(count);
                    }

                    stats.getUpdateProcessed().addAndGet(batch.size());
                    logger.info("Batch {} completed. Total processed so far: {}/{}", batchNumber, stats.getUpdateProcessed().get() + 1, total);
                } catch (Exception e) {
                    logger.error("Error uploading batch {}: {}", batchNumber, e.getMessage(), e);
                    batch.forEach((id, url) -> stats.getFailedUpdateMap().put(id, url));
                    stats.getUpdateFailed().addAndGet(batch.size());
                    stats.getUpdateBatchFailed().incrementAndGet();
                    stats.getUpdateFailedBatchList().add(String.valueOf(batchNumber));
                } finally {
                    semaphore.release();
                    stats.getUpdateBatchProcessed().incrementAndGet();
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
    }

    public List<Map<String, String>> splitMapIntoBatches(Map<String, String> original, int batchSize, int totalBatches, int total) {
        List<Map.Entry<String, String>> entries = new ArrayList<>(original.entrySet());

        List<Map<String, String>> batches = new ArrayList<>(totalBatches);

        for (int i = 0; i < total; i += batchSize) {
            int end = Math.min(i + batchSize, total);
            Map<String, String> batch = entries.subList(i, end).stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            batches.add(batch);
        }

        return batches;
    }

    private int updateBatchInShopify(Map<String, String> batch) {
        try {
            // Construct the GraphQL mutation input
            StringBuilder filesArrayBuilder = new StringBuilder();
            for (Map.Entry<String, String> entry : batch.entrySet()) {
                String fileId = entry.getKey();
                String s3Url = entry.getValue();
                filesArrayBuilder.append("{")
                        .append("id: \"").append(fileId).append("\", ")
                        .append("originalSource: \"").append(s3Url).append("\"")
                        .append("},");
            }

            if (filesArrayBuilder.isEmpty()) {
                logger.warn("Empty batch, skipping update.");
                return 0;
            }

            // Remove trailing comma
            String filesArray = filesArrayBuilder.substring(0, filesArrayBuilder.length() - 1);

            String mutation = """
                    mutation fileUpdate {
                      fileUpdate(files: [%s]) {
                        files {
                          id
                          alt
                          fileStatus
                          ... on MediaImage {
                            image {
                              url
                            }
                          }
                          ... on GenericFile {
                            url
                          }
                        }
                        userErrors {
                          field
                          message
                          code
                        }
                      }
                    }
                    """.formatted(filesArray);

            // Execute the mutation
            JSONObject response = executeGraphQLQuery(mutation);

            if (!response.has("data") || response.isNull("data")) {
                logger.error("Update response is missing 'data'; possible API error.");
                return 0;
            }

            JSONObject fileUpdate = response.getJSONObject("data").optJSONObject("fileUpdate");
            if (fileUpdate == null || fileUpdate.has("userErrors") && !fileUpdate.getJSONArray("userErrors").isEmpty()) {
                JSONArray errors = fileUpdate != null ? fileUpdate.getJSONArray("userErrors") : new JSONArray();
                for (int i = 0; i < errors.length(); i++) {
                    JSONObject error = errors.getJSONObject(i);
                    logger.error("Shopify Update Error: field={}, code={}, message={}",
                            error.optJSONArray("field"),
                            error.optString("code"),
                            error.optString("message"));
                }
                return 0;
            }

            JSONArray updatedFiles = fileUpdate.getJSONArray("files");
            logger.info("Successfully updated {} files in Shopify", updatedFiles.length());
            return updatedFiles.length();

        } catch (Exception e) {
            logger.error("Exception during fileUpdate batch call", e);
            return 0;
        }
    }

    private void createImagesAsync(List<String> imageUrls, SkuStats stats) {
        stats.getFailedCreateList().addAll(imageUrls);
//        int MAX_CONCURRENT_BATCHES = 10;
//        Semaphore semaphore = new Semaphore(MAX_CONCURRENT_BATCHES);
//        ExecutorService executorService = Executors.newFixedThreadPool(MAX_CONCURRENT_BATCHES);
//
//        int batchSize = 250;
//        int total = imageUrls.size();
//        logger.info("Total URLs count: {}", total);
//        int totalBatches = (int) Math.ceil((double) total / batchSize);
//        stats.getCreateTotalBatch().addAndGet(totalBatches);
//
//        List<Future<?>> futures = new ArrayList<>();
//
//        for (int i = 0; i < imageUrls.size(); i += batchSize) {
//            final int batchNumber = (i / batchSize) + 1;
//            final List<String> batch = imageUrls.subList(i, Math.min(i + batchSize, imageUrls.size()));
//            logger.info("Starting batch {} of {} with {} images...", batchNumber, totalBatches, batch.size());
//            futures.add(executorService.submit(() -> {
//                try {
//                    semaphore.acquire();
//                    productMigrationService.regulateApiRate();
//                    logger.info("Starting batch {} of {} with {} images...", batchNumber, totalBatches, batch.size());
//                    remainingPoints.addAndGet(-API_COST_PER_CALL);
//                    int count = registerBatchInShopify(batch);
//                    if (count == 0) {
//                        stats.getCreateBatchFailed().incrementAndGet();
//                        stats.getCreateFailedBatchList().add(String.valueOf(batchNumber));
//                        stats.getFailedCreateList().addAll(batch);
//                        stats.getCreateFailed().addAndGet(batch.size());
//                    } else {
//                        stats.getCreateBatchSuccess().incrementAndGet();
//                        stats.getCreateSuccess().addAndGet(count);
//                    }
//
//                    stats.getCreateProcessed().addAndGet(batch.size());
//                    logger.info("Batch {} completed. Total processed so far: {}/{}", batchNumber, stats.getCreateBatchProcessed().get() + 1, imageUrls.size());
//                } catch (Exception e) {
//                    logger.error("Error uploading batch {}: {}", batchNumber, e.getMessage(), e);
//                    stats.getCreateBatchFailed().incrementAndGet();
//                    stats.getCreateFailedBatchList().add(String.valueOf(batchNumber));
//                    stats.getFailedCreateList().addAll(batch);
//                    stats.getCreateFailed().addAndGet(batch.size());
//                } finally {
//                    semaphore.release();
//                    stats.getCreateBatchProcessed().incrementAndGet();
//                }
//            }));
//
//        }
//
//        for (Future<?> future : futures) {
//            try {
//                future.get();
//            } catch (InterruptedException | ExecutionException e) {
//                logger.error("Batch execution interrupted: {}", e.getMessage(), e);
//            }
//        }
//
//        executorService.shutdown();
//        try {
//            if (!executorService.awaitTermination(5, TimeUnit.MINUTES)) {
//                logger.warn("Executor did not terminate in the specified time.");
//                executorService.shutdownNow();
//                if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
//                    logger.error("Executor did not terminate after forced shutdown.");
//                }
//            } else {
//                logger.info("All batches completed successfully within 5 minutes.");
//            }
//        } catch (InterruptedException e) {
//            logger.error("Shutdown interrupted: {}", e.getMessage(), e);
//            executorService.shutdownNow();
//            Thread.currentThread().interrupt();
//        }
    }

    String buildShopifyFileQuery(String sku, String cursor) {
        String afterClause = (cursor != null && !cursor.isEmpty())
                ? String.format(", after: \"%s\"", cursor)
                : "";

        return """
                {
                  files(first: 250, query: "filename:_%s_", sortKey: CREATED_AT%s) {
                    edges {
                      cursor
                      node {
                        __typename
                        ... on GenericFile {
                          id
                          url
                          alt
                          createdAt
                        }
                        ... on MediaImage {
                          id
                          createdAt
                          alt
                          image {
                            url
                            altText
                          }
                        }
                      }
                    }
                    pageInfo {
                      hasNextPage
                      endCursor
                    }
                  }
                }
                """.formatted(sku, afterClause);
    }

    public String extractCleanFilename(String url) {
        if (url == null || url.isBlank()) return "";

        // Remove query params
        int queryIndex = url.indexOf("?");
        String cleanUrl = (queryIndex > -1) ? url.substring(0, queryIndex) : url;

        // Extract only the 'product_*.ext' part
        int productIdx = cleanUrl.indexOf("/product");
        if (productIdx == -1) return "";

        return cleanUrl.substring(productIdx + 1); // remove leading '/'
    }

    private JSONObject executeGraphQLQuery(String query) {
        try {
            JSONObject requestBody = new JSONObject();
            requestBody.put("query", query);

            HttpHeaders headers = new HttpHeaders();
            headers.set("X-Shopify-Access-Token", accessToken);
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> entity = new HttpEntity<>(requestBody.toString(), headers);

            logger.info("Sending Shopify GraphQL request:");
            logger.info("URL: " + shopifyStore + "/admin/api/2025-01/graphql.json");
            logger.info("Headers: " + headers.entrySet().stream()
                    .map(e -> e.getKey() + ": " + (e.getKey().equals("X-Shopify-Access-Token") ? "****" : e.getValue()))
                    .collect(Collectors.joining(", ")));
            logger.info("Request Body: " + requestBody.toString(2));  // Pretty-print JSON

            ResponseEntity<String> responseEntity = restTemplate.exchange(
                    shopifyStore + "/admin/api/2025-01/graphql.json",
                    HttpMethod.POST,
                    entity,
                    String.class
            );

            // Log Response Headers and Body
            HttpHeaders responseHeaders = responseEntity.getHeaders();
            String responseBody = responseEntity.getBody();
            String requestId = responseHeaders.getFirst("x-request-id");

            logger.info("Received Response:");
            logger.info("Status: " + responseEntity.getStatusCode());
            logger.info("Headers: " + responseHeaders);
            logger.info("X-Request-ID: " + requestId);
            logger.info("Response Body: " + responseBody);

            return new JSONObject(responseBody);

        } catch (HttpClientErrorException e) {
            logger.error("API error: " + e.getMessage());
            return new JSONObject();
        }
    }

    public Map<String, Object> getImageJobSummarySku() {
        Map<String, Object> response = new LinkedHashMap<>();

        response.put("totalSkus", totalSkus.get());
        response.put("skusProcessed", skusProcessed.get());
        response.put("skusSucceeded", skusSucceeded.get());
        response.put("skusFailed", skusFailed.get());

        List<Map<String, Object>> skuDetails = skuStatsMap.entrySet().stream()
                .map(entry -> {
                    String sku = entry.getKey();
                    SkuStats stats = entry.getValue();
                    Map<String, Object> skuInfo = new LinkedHashMap<>();
                    skuInfo.put("sku", sku);
                    skuInfo.put("imagesFetched", stats.getImagesFetched().get());
                    skuInfo.put("imagesToUpdate", stats.getImagesToUpdate().get());
                    skuInfo.put("updateProcessed", stats.getUpdateProcessed().get());
                    skuInfo.put("updateSuccess", stats.getUpdateSuccess().get());
                    skuInfo.put("updateFailed", stats.getUpdateFailed().get());
                    skuInfo.put("updateTotalBatch", stats.getUpdateTotalBatch().get());
                    skuInfo.put("updateBatchProcessed", stats.getUpdateBatchProcessed().get());
                    skuInfo.put("updateBatchSuccess", stats.getUpdateBatchSuccess().get());
                    skuInfo.put("updateBatchFailed", stats.getUpdateBatchFailed().get());
                    skuInfo.put("updateFailedBatchList", stats.getUpdateFailedBatchList());
                    skuInfo.put("failedUpdateMap", stats.getFailedUpdateMap());

                    skuInfo.put("imagesToCreate", stats.getImagesToCreate().get());
                    skuInfo.put("createProcessed", stats.getCreateProcessed().get());
                    skuInfo.put("createSuccess", stats.getCreateSuccess().get());
                    skuInfo.put("createFailed", stats.getCreateFailed().get());
                    skuInfo.put("createTotalBatch", stats.getCreateTotalBatch().get());
                    skuInfo.put("createBatchProcessed", stats.getCreateBatchProcessed().get());
                    skuInfo.put("createBatchSuccess", stats.getCreateBatchSuccess().get());
                    skuInfo.put("createBatchFailed", stats.getCreateBatchFailed().get());
                    skuInfo.put("createFailedBatchList", stats.getCreateFailedBatchList());
                    skuInfo.put("failedCreateUrls", stats.getFailedCreateList());

                    return skuInfo;
                }).collect(Collectors.toList());

        response.put("skuDetails", skuDetails);

        return response;
    }
}
