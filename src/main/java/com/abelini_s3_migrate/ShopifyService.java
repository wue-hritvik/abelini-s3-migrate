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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class ShopifyService {
    private static final Logger logger = LoggerFactory.getLogger(ShopifyService.class);
    private final RestTemplate restTemplate = new RestTemplate();
    private final Tika tika = new Tika();
    private final ObjectMapper objectMapper;

    @Value("${shopify.store}")
    private String shopifyStore;

    @Value("${shopify.access.token}")
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

    public void uploadImagesToShopify(String csvFilePath) throws IOException, CsvException {
        logger.info("Starting bulk upload to Shopify...");
        List<String> imageUrls = readCSV(csvFilePath);
        int batchSize = 100; // Send 100 files per batch
        ExecutorService executor = Executors.newFixedThreadPool(5); // Parallel execution

        int remainingCost = 2000; // Start with max available cost

        for (int i = 0; i < imageUrls.size(); i += batchSize) {
            List<String> batch = imageUrls.subList(i, Math.min(i + batchSize, imageUrls.size()));

            executor.submit(() -> {
                try {
                    registerBatchInShopify(batch);
                } catch (Exception e) {
                    logger.error("Error uploading batch: {}", e.getMessage(), e);
                }
            });

            // Reduce available cost
            remainingCost -= 20;

            // If approaching the limit, wait for restore
            if (remainingCost < 200) {
                logger.warn("Approaching API rate limit, sleeping for 2 seconds...");
                try {
                    Thread.sleep(2000);
                    remainingCost += 200; // Restore 100 points per second
                    if (remainingCost > 2000) remainingCost = 2000; // Cap at 2000
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        logger.info("Bulk upload completed.");
    }

    public void registerBatchInShopify(List<String> fileUrls) {
        List<Map<String, String>> filesList = new ArrayList<>();
        for (String fileUrl : fileUrls) {
            String encodedUrl = encodeUrl(fileUrl);
            String fileName = generateShopifyFilePath(fileUrl);
            String contentType = detectShopifyContentType(fileUrl);
            if (notSupportedFileType(fileUrl)) continue;
            logger.info("contentType ::: {}", contentType);
            Map<String, String> fileEntry = new HashMap<>();
            fileEntry.put("originalSource", encodedUrl);
            fileEntry.put("filename", fileName);
            fileEntry.put("alt", fileName);
            fileEntry.put("contentType", contentType);
            filesList.add(fileEntry);
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
            if (response.contains("\"userErrors\":[")) {  // Instead of just checking if "userErrors" exists
                if (!response.contains("\"userErrors\":[]")) { // Only log if errors are present
                    logger.error("Error uploading batch: {}", response);
                } else {
                    logger.info("Batch uploaded successfully.");
                }
            }
        } catch (Exception e) {
            logger.error("Error in batch upload: {}", e.getMessage(), e);
        }
    }

    private static final Set<String> SUPPORTED_IMAGE_MIME_TYPES = Set.of(
            "image/png", "image/jpeg", "image/gif", "image/jpg"
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
        logger.info("file name ::: {}", fileName);
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

    private String detectShopifyContentType(String fileUrl) {
        String mimeType = detectMimeType(fileUrl);
        if ("application/pdf".equals(mimeType)) return "FILE";
        if (mimeType.startsWith("image/")) return "IMAGE";
        if (mimeType.startsWith("video/")) return "VIDEO";

        return "FILE";
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
