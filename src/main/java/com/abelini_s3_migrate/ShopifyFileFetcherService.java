package com.abelini_s3_migrate;

import com.opencsv.CSVWriter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

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

}
