package com.abelini_s3_migrate;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.tika.Tika;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class ShopifyFileFetcherService {

    @Value("${shopify_store}")
    private String shopifyStore;

    @Value("${shopify_access_token}")
    private String ACCESS_TOKEN;
    private final String SHOPIFY_GRAPHQL_URL = "/admin/api/2025-01/graphql.json";
    private static final String CSV_FILE_PATH = "src/main/resources/s3file/shopify_filename.csv";
    private static final String CSV_FILE_PATH_BULK = "src/main/resources/s3file/shopify_filename_bulk.csv";
    private static final int API_COST_PER_CALL = 35;
    private static final int MAX_POINTS = 2000;
    private static final int RECOVERY_RATE = 100;
    private static final int SAFE_THRESHOLD = 200;
    private static final AtomicInteger remainingPoints = new AtomicInteger(MAX_POINTS);
    private static final AtomicInteger totalFilesStored = new AtomicInteger(0);
    private static final AtomicInteger batchNumber = new AtomicInteger(1); // AtomicInteger for thread-safe batch number
    private static final Logger LOGGER = Logger.getLogger(ShopifyFileFetcherService.class.getName());
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ShopifyFileFetcherService.class);
    private final RestTemplate restTemplate = new RestTemplate();

    public ShopifyFileFetcherService() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(10);
        taskExecutor.setMaxPoolSize(20);
        taskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        taskExecutor.initialize();
    }

    @Async
    public void fetchAndStoreShopifyFiles() {
        LOGGER.info("Starting Shopify file fetching process... at " + ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z")));

        List<String[]> fileData = new ArrayList<>();
        String cursor = null;
        boolean hasNextPage;

        // Prepare file and add header only if file is new or empty
        fileData.add(new String[]{"filename"});
        writeToCSV(fileData, true);  // Pass 'true' to indicate this is the header write
        fileData.clear();

        do {
            int currentBatchNumber = batchNumber.getAndIncrement();
            LOGGER.info("Batch " + currentBatchNumber + " started.");

            // Build the 'after' clause if a cursor is available.
            String afterClause = (cursor != null && !cursor.isEmpty()) ? String.format(", after: \"%s\"", cursor) : "";

            String query = """
                    {
                        files(first: 250, sortKey: CREATED_AT, query: "created_at:>=2025-02-16"%s) {
                            edges {
                                node {
                                    preview {
                                        image {
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
                    """.formatted(afterClause);

            try {
                JSONObject response = executeGraphQLQuery(query);
                JSONObject filesObject = response.getJSONObject("data").getJSONObject("files");
                JSONArray edges = filesObject.getJSONArray("edges");
                hasNextPage = filesObject.getJSONObject("pageInfo").getBoolean("hasNextPage");
                cursor = filesObject.getJSONObject("pageInfo").optString("endCursor", null);

                // Process each file
                for (int i = 0; i < edges.length(); i++) {
                    String altText = edges.getJSONObject(i)
                            .getJSONObject("node")
                            .getJSONObject("preview")
                            .getJSONObject("image")
                            .optString("altText", "");
                    fileData.add(new String[]{altText});
                    totalFilesStored.incrementAndGet();  // Thread-safe increment
                }

                LOGGER.info("Total files stored so far: " + totalFilesStored.get());

                regulateApiRate();

                // Write to CSV every 5 batches
                if (currentBatchNumber % 5 == 0) {
                    writeToCSV(fileData, false);  // Pass 'false' to not write header again
                    fileData.clear();
                }
            } catch (Exception e) {
                LOGGER.severe("Error fetching batch " + currentBatchNumber + ": " + e.getMessage());
                break;
            }

            LOGGER.info("Batch " + currentBatchNumber + " completed.");
        } while (hasNextPage);

        // Write any remaining data to CSV
        writeToCSV(fileData, false);
        LOGGER.info("Completed Shopify file fetching process. Total files stored: " + totalFilesStored.get() + " ,ended at: " + ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z")));
    }

    private JSONObject executeGraphQLQuery(String query) {
        try {
            JSONObject requestBody = new JSONObject();
            requestBody.put("query", query);

            HttpHeaders headers = new HttpHeaders();
            headers.set("X-Shopify-Access-Token", ACCESS_TOKEN);
            headers.setContentType(MediaType.APPLICATION_JSON);

            // Wrap the request body and headers into an HttpEntity
            HttpEntity<String> entity = new HttpEntity<>(requestBody.toString(), headers);

            String response = restTemplate.postForObject(shopifyStore + SHOPIFY_GRAPHQL_URL, entity, String.class);
            remainingPoints.addAndGet(-API_COST_PER_CALL);
            return new JSONObject(response);
        } catch (HttpClientErrorException e) {
            LOGGER.severe("API error: " + e.getMessage());
            return new JSONObject();
        }
    }

    private void writeToCSV(List<String[]> data, boolean isHeader) {
        File file = new File(CSV_FILE_PATH);
        try (CSVWriter writer = new CSVWriter(new FileWriter(file, true))) {
            // Write the header only if the file is new or empty
//            if (isHeader && !file.exists() || file.length() == 0) {
//                writer.writeNext(new String[]{"filename"});
//            }
            writer.writeAll(data);
            LOGGER.info("CSV file updated: " + CSV_FILE_PATH);
        } catch (IOException e) {
            LOGGER.severe("Error writing CSV file: " + e.getMessage());
        }
    }

    private void regulateApiRate() {
        if (remainingPoints.get() < SAFE_THRESHOLD) {
            int waitTime = Math.min(5, (MAX_POINTS - remainingPoints.get()) / RECOVERY_RATE);
            LOGGER.info("Low API points (" + remainingPoints.get() + "), pausing for " + waitTime + " seconds to recover.");
            try {
                TimeUnit.SECONDS.sleep(waitTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            remainingPoints.addAndGet(waitTime * RECOVERY_RATE);  // Thread-safe increment
        }
    }

    @Async
    public void fetchAndStoreShopifyFilesBulk() {
        LOGGER.info("Starting bulk operation for file alt texts... at: " + ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z")));

        // Step 1: Initiate the bulk operation with a mutation.
        String mutation = """
                mutation {
                  bulkOperationRunQuery(
                    query: "{ files(query: \\"created_at:>=2025-02-16\\") { edges { node { preview { image { altText } } } } } }"
                  ) {
                    bulkOperation {
                      id
                      status
                    }
                    userErrors {
                      field
                      message
                    }
                  }
                }
                """;

        JSONObject startResponse = executeGraphQLQuery(mutation);
        if (startResponse.has("errors") || startResponse
                .optJSONObject("data")
                .optJSONObject("bulkOperationRunQuery")
                .optJSONArray("userErrors").length() > 0) {
            LOGGER.severe("Error starting bulk operation: " + startResponse);
            return;
        }

        LOGGER.info("Bulk operation initiated. Polling for completion...");

        // Step 2: Poll for the bulk operation completion.
        JSONObject bulkInfo = pollBulkOperation();
        if (bulkInfo == null) {
            LOGGER.severe("Bulk operation did not complete successfully.");
            return;
        }

        if (!"COMPLETED".equals(bulkInfo.optString("status"))) {
            LOGGER.severe("Bulk operation failed with status: " + bulkInfo.optString("status"));
            return;
        }

        String fileUrl = bulkInfo.optString("url");
        if (fileUrl == null || fileUrl.isEmpty()) {
            LOGGER.severe("Bulk operation completed, but no file URL was returned.");
            return;
        }

        LOGGER.info("Bulk operation completed. Downloading file from: " + fileUrl);

        // Step 3: Process the bulk file using streaming.
        processBulkFileStream(fileUrl);
    }

    /**
     * Polls the bulk operation status until it's completed.
     *
     * @return a JSONObject with bulk operation information.
     */
    private JSONObject pollBulkOperation() {
        String query = """
                {
                  currentBulkOperation {
                    id
                    status
                    errorCode
                    createdAt
                    completedAt
                    objectCount
                    fileSize
                    url
                  }
                }
                """;

        while (true) {  // Infinite loop, will break when operation completes or fails
            try {
                TimeUnit.SECONDS.sleep(10);  // Wait for 10 seconds before polling again
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.severe("Polling interrupted: " + e.getMessage());
                return null;
            }

            LOGGER.info("Calling Shopify GraphQL API to check bulk operation status...");

            // Call the API to check the status of the bulk operation
            JSONObject response = executeGraphQLQueryBulk(query);

            LOGGER.info("Received response from Shopify API.");

            JSONObject bulkOperation = response.optJSONObject("data").optJSONObject("currentBulkOperation");

            if (bulkOperation == null) {
                LOGGER.warning("Bulk operation info not available yet.");
            } else {
                String status = bulkOperation.optString("status");
                LOGGER.info("Current bulk operation status: " + status);

                if ("COMPLETED".equals(status)) {
                    LOGGER.info("Bulk operation completed successfully.");
                    return bulkOperation;  // Return the completed bulk operation
                } else if ("FAILED".equals(status)) {
                    LOGGER.severe("Bulk operation failed with error: " + bulkOperation.optString("errorCode"));
                    return null;  // Return null in case of failure
                }
            }
        }
    }

    /**
     * Processes the bulk file by streaming its content line by line and writing to CSV.
     *
     * @param fileUrl the URL of the bulk file.
     */
    private void processBulkFileStream(String fileUrl) {
        // Ensure the output directory exists
        File outputFile = new File(CSV_FILE_PATH_BULK);
        outputFile.getParentFile().mkdirs();  // Create the directories if they don't exist

        // Download and process the JSONL file (line-by-line processing)
        try {
            restTemplate.execute(fileUrl, HttpMethod.GET, null, response -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getBody()));
                     CSVWriter writer = new CSVWriter(new FileWriter(outputFile, true))) {

                    // Write the header if the file is new or empty
                    if (!outputFile.exists() || outputFile.length() == 0) {
                        writer.writeNext(new String[]{"altText"});
                    }

                    String line;
                    int processedCount = 0;
                    while ((line = reader.readLine()) != null) {
                        // Each line is a JSON object (JSONL format)
                        JSONObject jsonLine = new JSONObject(line);

                        // Log the first response line to check the structure
                        if (processedCount == 0) {
                            LOGGER.info("First response line: " + jsonLine.toString());
                        }

                        // The root object directly contains 'preview', not 'node'
                        JSONObject preview = jsonLine.optJSONObject("preview");

                        // Check if 'preview' exists
                        if (preview != null) {
                            JSONObject image = preview.optJSONObject("image");
                            if (image != null) {
                                String altText = image.optString("altText", "");
                                writer.writeNext(new String[]{altText});
                                processedCount++;
                            } else {
                                LOGGER.warning("Missing 'image' in preview.");
                            }
                        } else {
                            LOGGER.warning("Missing 'preview' in JSON object.");
                        }

                        // Optionally log progress every 100,000 records
                        if (processedCount % 100000 == 0) {
                            LOGGER.info("Processed " + processedCount + " records so far...");
                        }
                    }

                    LOGGER.info("Completed processing bulk file. Total records processed: " + processedCount + " ,at: " + ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z")));
                }
                return null;
            });
        } catch (Exception e) {
            LOGGER.severe("Error processing bulk file stream: " + e.getMessage());
        }
    }


    /**
     * Executes a GraphQL query/mutation with the necessary headers.
     *
     * @param query the GraphQL query or mutation.
     * @return the JSONObject response.
     */
    private JSONObject executeGraphQLQueryBulk(String query) {
        try {
            JSONObject requestBody = new JSONObject();
            requestBody.put("query", query);

            HttpHeaders headers = new HttpHeaders();
            headers.set("X-Shopify-Access-Token", ACCESS_TOKEN);
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> entity = new HttpEntity<>(requestBody.toString(), headers);
            ResponseEntity<String> responseEntity = restTemplate.postForEntity(shopifyStore + SHOPIFY_GRAPHQL_URL, entity, String.class);

            return new JSONObject(responseEntity.getBody());
        } catch (HttpClientErrorException e) {
            LOGGER.severe("API error: " + e.getMessage());
            return new JSONObject();
        }
    }

    private static final String S3_CSV_PATH = "src/main/resources/s3file/abelini_s3_urls.csv";
    private static final String BULK_CSV_PATH = "src/main/resources/s3file/shopify_filename_bulk.csv";
    private static final String MISSING_URLS_CSV = "src/main/resources/s3file/missing_image_urls.csv";
    private static final String OTHER_FILES_CSV = "src/main/resources/s3file/other_file_s3_urls.csv";

    // Supported MIME types for images.
    private static final Set<String> SUPPORTED_IMAGE_MIME_TYPES = Set.of(
            "image/png", "image/jpeg", "image/gif", "image/jpg", "image/webp", "image/svg+xml"
    );

    private final Tika tika = new Tika();

    @Async
    public CompletableFuture<Void> compareFileNames() {
        try {
            logger.info("Starting compareFileNames process ,at: {}", ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z")));

            // Start asynchronous S3 URL map creation.
            CompletableFuture<Map<String, String>> imageUrlMapTask = createImageUrlMapAsync(S3_CSV_PATH);

            // Read file names from the CSV (skipping header) into a Set for fast lookups.
            Set<String> fileNames = readCsvToSet(BULK_CSV_PATH, true);
            logger.info("Total file names processed: {}", fileNames.size());

            // Wait for the S3 URL map task to complete.
            Map<String, String> imageUrlMap = imageUrlMapTask.get();

            // Compare file names and get list of missing image URLs.
            List<String> missingUrls = findMissingUrls(fileNames, imageUrlMap);

            // Write the missing URLs asynchronously.
            writeCsvAsync(MISSING_URLS_CSV, missingUrls, "image_missing_urls");

            logger.info("compareFileNames process completed. Total missing URLs found: {}   ,at: {}", missingUrls.size(), ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z")));

        } catch (Exception e) {
            logger.error("Error in compareFileNames: ", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Async

    public CompletableFuture<Map<String, String>> createImageUrlMapAsync(String s3CsvPath) {
        logger.info("createImageUrlMapAsync started");
        Map<String, String> imageUrlMap = new HashMap<>();
        List<String> otherUrls = new ArrayList<>();

        AtomicInteger totalUrls = new AtomicInteger(0);
        AtomicInteger imageCount = new AtomicInteger(0);
        AtomicInteger otherCount = new AtomicInteger(0);

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(s3CsvPath))) {
            // Optionally skip header line if present.
            String firstLine = reader.readLine();
            if (firstLine != null && looksLikeHeader(firstLine)) {
                logger.info("Skipping header in S3 CSV: {}", firstLine);
            } else if (firstLine != null) {
                // Process the first line if it doesn't look like a header.
                processUrlLine(firstLine, imageUrlMap, otherUrls, totalUrls, imageCount, otherCount);
            }

            // Process remaining lines sequentially.
            String line;
            while ((line = reader.readLine()) != null) {
                processUrlLine(line, imageUrlMap, otherUrls, totalUrls, imageCount, otherCount);
            }
            logger.info("createImageUrlMapAsync started ended");
        } catch (IOException e) {
            logger.error("Error reading S3 CSV file: ", e);
        }

        logger.info("Total S3 URLs processed: {}. Image URLs: {}. Other URLs: {}.",
                totalUrls.get(), imageCount.get(), otherCount.get());

        // Write the other URLs asynchronously.
        writeCsvAsync(OTHER_FILES_CSV, otherUrls, "other_file_urls");

        return CompletableFuture.completedFuture(imageUrlMap);
    }

    /**
     * Processes a single CSV line by checking if it is a supported image URL.
     *
     * @param line        the CSV line.
     * @param imageUrlMap map to store fileName to URL mapping.
     * @param otherUrls   list to store URLs that are not supported images.
     * @param totalUrls   counter for total processed URLs.
     * @param imageCount  counter for supported image URLs.
     * @param otherCount  counter for other URLs.
     */
    private void processUrlLine(String line, Map<String, String> imageUrlMap, List<String> otherUrls,
                                AtomicInteger totalUrls, AtomicInteger imageCount, AtomicInteger otherCount) {
        totalUrls.incrementAndGet();
        String url = line.trim().replaceAll("^\"|\"$", "");

        // Check using Tika MIME detection.
        if (!isSupportedImage(url)) {
            otherUrls.add(url);
            otherCount.incrementAndGet();
        } else {
            String fileName = extractFileNameFromUrl(url);
            if (fileName != null) {
                imageUrlMap.put(fileName, url);
                imageCount.incrementAndGet();
            }
        }
    }

    /**
     * Reads a CSV file (optionally skipping the header) and returns a Set of trimmed lines.
     *
     * @param filePath   the CSV file path.
     * @param skipHeader true to skip the first header line.
     * @return a set of lines.
     * @throws IOException if an I/O error occurs.
     */
    public Set<String> readCsvToSet(String filePath, boolean skipHeader) throws IOException {
        logger.info("readCsvToSet started");
        AtomicInteger fileNameCount = new AtomicInteger(0);
        Set<String> fileNames = new HashSet<>();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath))) {
            if (skipHeader) {
                String header = reader.readLine();
                if (header != null) {
                    logger.info("Skipping header in file {}: {}", filePath, header);
                }
            }
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmedLine = line.trim().replaceAll("^\"|\"$", "");
                fileNames.add(trimmedLine);
                fileNameCount.incrementAndGet();
            }
        }
        logger.info("readCsvToSet ended");
        logger.info("Finished reading file names from {}. Count: {}", filePath, fileNameCount.get());
        return fileNames;
    }

    /**
     * Compares file names from a bulk file with the image URL map and returns URLs that are missing.
     *
     * @param fileNames   set of file names from the bulk file.
     * @param imageUrlMap map of image file names to URLs.
     * @return a list of missing URLs.
     */
    public List<String> findMissingUrls(Set<String> fileNames, Map<String, String> imageUrlMap) {
        logger.info("started comparing files");
        AtomicInteger comparisonCounter = new AtomicInteger(0);
        List<String> missingUrls = new ArrayList<>();

        for (Map.Entry<String, String> entry : imageUrlMap.entrySet()) {
            int count = comparisonCounter.incrementAndGet();
            if (count % 10000 == 0) {
                logger.info("Processed {} comparisons so far.", count);
            }
            if (!fileNames.contains(entry.getKey())) {
                missingUrls.add(entry.getValue());
            }
        }
        logger.info("file comparison completed");
        logger.info("Total comparisons made: {}", comparisonCounter.get());
        logger.info("Total missing URLs found: {}", missingUrls.size());
        return missingUrls;
    }

    /**
     * Asynchronously writes data to a CSV file.
     * If a header is provided and the file does not exist or lacks a header, the header is written.
     *
     * @param fileName the path of the file to write.
     * @param data     list of lines to write.
     * @param header   header string to write (or null if none).
     */
    @Async
    public void writeCsvAsync(String fileName, List<String> data, String header) {
        logger.info("started writing csv to async filename ::{}", fileName);
        CompletableFuture.runAsync(() -> {
            try {
                Path filePath = Paths.get(fileName);
                // Ensure the parent directory exists.
                if (filePath.getParent() != null) {
                    Files.createDirectories(filePath.getParent());
                }

                boolean writeHeader = false;
                if (header != null) {
                    if (!Files.exists(filePath)) {
                        writeHeader = true;
                    } else {
                        // Check if the file already has a header.
                        try (BufferedReader br = Files.newBufferedReader(filePath)) {
                            String firstLine = br.readLine();
                            if (firstLine == null || !firstLine.trim().equals(header)) {
                                writeHeader = true;
                            }
                        }
                    }
                }

                try (BufferedWriter writer = Files.newBufferedWriter(filePath,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                    if (writeHeader) {
                        writer.write(header);
                        writer.newLine();
                    }
                    for (String line : data) {
                        writer.write(line);
                        writer.newLine();
                    }
                }
                logger.info("Finished writing file: {} with {} records.", fileName, data.size());
            } catch (IOException e) {
                logger.error("Error writing CSV file: " + fileName, e);
            }
        });
    }

    /**
     * Extracts a file name from a URL by taking the substring after ".com/" and replacing "/" with "_".
     *
     * @param fileUrl the URL string.
     * @return the extracted file name, or null if extraction fails.
     */
    public String extractFileNameFromUrl(String fileUrl) {
        try {
            return fileUrl.substring(fileUrl.indexOf(".com/") + 5)
                    .replace("/", "_")
                    .replaceAll("\\s+", "_");
        } catch (Exception e) {
            logger.error("Error extracting file name from URL: " + fileUrl, e);
            return null;
        }
    }

    /**
     * Uses Tika to detect the MIME type of the given filename.
     *
     * @param filename the filename or URL.
     * @return the detected MIME type.
     */
    public String detectMimeType(String filename) {
        try {
            return tika.detect(filename);
        } catch (Exception e) {
            logger.warn("Could not detect MIME type for {}. Defaulting to image/jpeg", filename);
            return "image/jpeg";
        }
    }

    /**
     * Checks if the provided URL points to a supported image type.
     *
     * @param fileUrl the URL to check.
     * @return true if it is a supported image, false otherwise.
     */
    public boolean isSupportedImage(String fileUrl) {
        String lowerUrl = fileUrl.toLowerCase();
        // Quick check based on file extension.
        if (lowerUrl.endsWith(".png") || lowerUrl.endsWith(".jpg") || lowerUrl.endsWith(".jpeg") ||
                lowerUrl.endsWith(".gif") || lowerUrl.endsWith(".webp") || lowerUrl.endsWith(".svg")) {
            return true;
        }
        // Fallback: detect MIME type.
        String mimeType = detectMimeType(fileUrl);
        return SUPPORTED_IMAGE_MIME_TYPES.contains(mimeType);
    }

    /**
     * Determines if a given line looks like a header.
     * (If it does not start with "http" and is not empty.)
     *
     * @param line the line to check.
     * @return true if the line is considered a header.
     */
    public boolean looksLikeHeader(String line) {
        return !line.toLowerCase().startsWith("http") && !line.trim().isEmpty();
    }
}
