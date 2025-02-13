package com.abelini_s3_migrate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.commons.io.FileUtils;
import org.apache.tika.Tika;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.UrlResource;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class ShopifyService {
    private static final Logger logger = LoggerFactory.getLogger(ShopifyService.class);
    private final RestTemplate restTemplate = new RestTemplate();
    private final Tika tika = new Tika();

    @Value("${shopify.store}")
    private String shopifyStore;

    @Value("${shopify.access.token}")
    private String accessToken;

    private List<String> readCSV(String filePath) throws IOException, CsvException {
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            List<String[]> records = reader.readAll();
            return records.stream().skip(1) // Skip header row
                    .map(row -> row[0])
                    .collect(Collectors.toList());
        }
    }

    private String detectMimeType(String filename) {
        try {
            return tika.detect(filename);
        } catch (Exception e) {
            logger.warn("Could not detect MIME type for {}. Defaulting to image/jpeg", filename);
            return "image/jpeg";
        }
    }

    private String sendGraphQLRequest(String query) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("X-Shopify-Access-Token", accessToken.trim());
            headers.set("Content-Type", "application/json");

            String requestBody = String.format("{\"query\": \"%s\"}", query.replace("\n", " ").replace("\"", "\\\""));
            HttpEntity<String> request = new HttpEntity<>(requestBody, headers);

            ResponseEntity<String> response = restTemplate.postForEntity(
                    shopifyStore + "/admin/api/2024-01/graphql.json",
                    request,
                    String.class
            );

            logger.info("Shopify Response: {}", response.getBody());
            return response.getBody();
        } catch (Exception e) {
            logger.error("Error sending GraphQL request: {}", e.getMessage());
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
        // Extract path after amazonaws.com/
        String relativePath = fileUrl.substring(fileUrl.indexOf(".com/") + 5);

        // Replace / with _
        relativePath = relativePath.replace("/", "_");

        // Replace multiple spaces with a single _
        relativePath = Pattern.compile("\\s+").matcher(relativePath).replaceAll("_");

        return relativePath;
    }

    private String extractFileId(String response) {
        try {
            JsonNode rootNode = new ObjectMapper().readTree(response);
            JsonNode fileNode = rootNode.path("data").path("fileCreate").path("files").get(0);
            return fileNode != null ? fileNode.path("id").asText() : null;
        } catch (Exception e) {
            logger.error("Error extracting file ID: {}", e.getMessage());
            return null;
        }
    }

//todo --------------------------------------------------------------------------------------

    public void uploadImagesToShopify(String csvFilePath) throws IOException, CsvException {
        logger.info("Registering S3 images in Shopify Files...");

        List<String> imageUrls = readCSV(csvFilePath);

        for (String url : imageUrls) {
            logger.info("Registering image: {}", url);
            registerS3UrlInShopify(url);
//            String fileId = registerS3UrlInShopify(url);
//            String formattedFileName = generateShopifyFilePath(url);
//            if (fileId != null) {
//                saveFilenameInMetafield(fileId, formattedFileName);
//            }
        }
    }

    //
//    private void saveFilenameInMetafield(String fileId, String customFilename) {
//        logger.info("saving file in with meta fields");
//        String query = String.format("""
//                    mutation {
//                        metafieldCreate(input: {
//                            ownerId: "%s",
//                            namespace: "custom_files",
//                            key: "original_filename",
//                            value: "%s",
//                            type: "single_line_text_field"
//                        }) {
//                            metafield {
//                                id
//                            }
//                            userErrors {
//                                field
//                                message
//                            }
//                        }
//                    }
//                """, fileId, customFilename);
//
//        sendGraphQLRequest(query);
//        logger.info("Stored custom filename '{}' for file ID {}", customFilename, fileId);
//    }
//
    private String registerS3UrlInShopify(String fileUrl) {
        String encodedUrl = encodeUrl(fileUrl);


        String query = String.format("""
                    mutation {
                        fileCreate(files: [{originalSource: "%s"}]) {
                            userErrors {
                                field
                                message
                            }
                        }
                    }
                """, encodedUrl);

        String response = sendGraphQLRequest(query);
        logger.info("Uploaded S3 URL to Shopify: {}", fileUrl);
//        return extractFileId(response);
        return null;
    }

//todo    --------------------------------------------------------------

//    public void uploadImagesToShopify(String csvFilePath) throws IOException, CsvException {
//        logger.info("Registering S3 images in Shopify Files...");
//
//        List<String> imageUrls = readCSV(csvFilePath);
//        bulkUploadImagesToShopify(imageUrls);
//    }
//
//    public void bulkUploadImagesToShopify(List<String> imageUrls) {
//        for (String fileUrl : imageUrls) {
//            try {
//                String fileName = generateShopifyFilePath(fileUrl);
//                String mimeType = detectMimeType(fileUrl);
//
//                // Step 1: Get a staged upload URL from Shopify
//                Map<String, String> uploadDetails = requestStagedUpload(fileName, mimeType);
//                if (uploadDetails == null) {
//                    logger.error("Failed to get staged upload URL for file: {}", fileUrl);
//                    continue;
//                }
//
//                // Step 2: Upload the file to Shopify's temporary storage
//                uploadFileToShopify(fileUrl, uploadDetails.get("url"), uploadDetails);
//
//                // Step 3: Register the uploaded file in Shopify
//                String fileId = registerFileInShopify(uploadDetails.get("stagedUploadPath"), fileName);
//                if (fileId != null) {
//                    saveFilenameInMetafield(fileId, fileName);
//                }
//
//            } catch (Exception e) {
//                logger.error("Error processing file: {}", fileUrl, e);
//            }
//        }
//    }
//
//    /**
//     * Step 1: Request a staged upload URL from Shopify.
//     */
//    private Map<String, String> requestStagedUpload(String filename, String mimeType) {
//        String query = String.format("""
//                    mutation {
//                        stagedUploadsCreate(input: { fileSize: "%s", filename: "%s", mimeType: "%s", resource: FILE }) {
//                            stagedTargets {
//                                url
//                                parameters {
//                                    name
//                                    value
//                                }
//                            }
//                            userErrors {
//                                field
//                                message
//                            }
//                        }
//                    }
//                """, "5000000", filename, mimeType);
//
//        String response = sendGraphQLRequest(query);
//        logger.info("staging response:: {}", response);
//        return parseUploadResponse(response);
//    }
//
//    /**
//     * Step 2: Upload the file to Shopify's temporary storage.
//     */
//    private void uploadFileToShopify(String fileUrl, String stagedUrl, Map<String, String> parameters) throws IOException {
//        RestTemplate restTemplate = new RestTemplate();
//        HttpHeaders headers = new HttpHeaders();
//        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
//
//        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
//
//        // ✅ Add all required parameters from Shopify's response
//        parameters.forEach(body::add);
//
//        // ✅ Upload file from S3
//        body.add("file", new UrlResource(fileUrl));
//
//        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);
//
//        try {
//            ResponseEntity<String> response = restTemplate.exchange(stagedUrl, HttpMethod.POST, requestEntity, String.class);
//            logger.info("File uploaded successfully to Shopify staged URL: {}", stagedUrl);
//            logger.info("Shopify Upload Response: {}", response.getBody());
//        } catch (HttpClientErrorException e) {
//            logger.error("Error uploading file to Shopify staged URL: {} - Response: {}", stagedUrl, e.getResponseBodyAsString());
//        } catch (Exception e) {
//            logger.error("Unexpected error uploading file to Shopify: {}", e.getMessage());
//        }
//    }
//
//    /**
//     * Step 3: Register the uploaded file in Shopify Files.
//     */
//    private String registerFileInShopify(String stagedUploadPath, String filename) {
//        String query = String.format("""
//                    mutation {
//                        fileCreate(files: [{stagedUploadPath: "%s", filename: "%s"}]) {
//                            files {
//                                id
//                            }
//                            userErrors {
//                                field
//                                message
//                            }
//                        }
//                    }
//                """, stagedUploadPath, filename);
//
//        String response = sendGraphQLRequest(query);
//        return extractFileId(response);
//    }
//
//    /**
//     * Step 4: Store a custom filename as a metafield for the uploaded file.
//     */
//    private void saveFilenameInMetafield(String fileId, String customFilename) {
//        String query = String.format("""
//                    mutation {
//                        metafieldCreate(input: {
//                            ownerId: "%s",
//                            namespace: "custom_files",
//                            key: "original_filename",
//                            value: "%s",
//                            type: "single_line_text_field"
//                        }) {
//                            metafield {
//                                id
//                            }
//                            userErrors {
//                                field
//                                message
//                            }
//                        }
//                    }
//                """, fileId, customFilename);
//
//        sendGraphQLRequest(query);
//        logger.info("Stored custom filename '{}' for file ID {}", customFilename, fileId);
//    }
//
//    /**
//     * Parses the response from stagedUploadsCreate to extract the upload URL.
//     */
//    private Map<String, String> parseUploadResponse(String response) {
//        try {
//            JsonNode rootNode = new ObjectMapper().readTree(response);
//            JsonNode stagedTarget = rootNode.path("data").path("stagedUploadsCreate").path("stagedTargets").get(0);
//
//            if (stagedTarget != null) {
//                Map<String, String> result = new java.util.HashMap<>();
//                result.put("url", stagedTarget.path("url").asText());
//                result.put("stagedUploadPath", stagedTarget.path("stagedUploadPath").asText());
//
//                // Extract parameters
//                JsonNode parameters = stagedTarget.path("parameters");
//                for (JsonNode param : parameters) {
//                    result.put(param.path("name").asText(), param.path("value").asText());
//                }
//                return result;
//            }
//        } catch (Exception e) {
//            logger.error("Error parsing staged upload response: {}", e.getMessage());
//        }
//        return null;
//    }

//todo ------------------------------------------------------------

//    private final String API_KEY = accessToken;
//    private final String SHOPIFY_APP_UPLOAD_URL= "https://s3migrate.myshopify.com/admin/api/2023-01/graphql.json";
//
//    public void processCsv(String csvFilePath) throws Exception {
//        List<ImageUploadRequest> images = readCsv(csvFilePath);
//        String TEMP_DIR = "src/main/resources/temp/";
//        File tempFolder = new File(TEMP_DIR);
//        if (!tempFolder.exists()) tempFolder.mkdirs();  // Ensure temp folder exists
//
//        for (ImageUploadRequest image : images) {
//            System.out.println("Processing: " + image.getS3Url());
//
//            // Download image to temporary folder
//            File localFile = downloadImage(image.getS3Url(), TEMP_DIR + image.getCustomFilename());
//
//            if (localFile != null && localFile.exists()) {
//                logger.info("local file not null");
//                // Upload to Shopify
//                String shopifyFileUrl = uploadImage(localFile, image.getCustomFilename(), image.getS3Url());
//                System.out.println("Uploaded: " + shopifyFileUrl);
//
//                // Delete the file after upload
//                Files.deleteIfExists(localFile.toPath());
//                System.out.println("Deleted temporary file: " + localFile.getAbsolutePath());
//            }else{
//                logger.info("local file is null");
//            }
//        }
//
//        // Cleanup temp folder if empty
//        if (Objects.requireNonNull(tempFolder.list()).length == 0) {
//            FileUtils.deleteDirectory(tempFolder);
//            System.out.println("Temp folder cleaned up.");
//        }
//    }
//
//    private List<ImageUploadRequest> readCsv(String filePath) throws Exception {
//        List<ImageUploadRequest> imageList = new ArrayList<>();
//        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
//            String[] line;
//            reader.readNext(); // Skip header
//            while ((line = reader.readNext()) != null) {
//                String url = line[0];
//                String s3Url = encodeUrl(url);
//                String customFilename = generateShopifyFilePath(s3Url);
//                imageList.add(new ImageUploadRequest(s3Url, customFilename));
//            }
//        }
//        return imageList;
//    }
//
//    private File downloadImage(String imageUrl, String outputPath) {
//        try (InputStream in = new URL(imageUrl).openStream()) {
//            File file = new File(outputPath);
//            Files.copy(in, file.toPath());
//            System.out.println("Downloaded: " + file.getAbsolutePath());
//            return file;
//        } catch (IOException e) {
//            System.err.println("Failed to download: " + imageUrl);
//            return null;
//        }
//    }
//
//    public String uploadImage(File imageFile, String customFilename, String s3Url) {
//        try {
//            logger.info("Uploading file to Shopify");
//
//            RestTemplate restTemplate = new RestTemplate();
//            HttpHeaders headers = new HttpHeaders();
//            headers.set("X-Shopify-Access-Token", API_KEY);
//            headers.setContentType(MediaType.APPLICATION_JSON);
//
//            // Convert image to Base64
//            byte[] fileContent = Files.readAllBytes(imageFile.toPath());
//            String base64String = Base64.getEncoder().encodeToString(fileContent);
//
//            // Create GraphQL request
//            JSONObject jsonRequest = new JSONObject();
//            jsonRequest.put("query", """
//                mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
//                  stagedUploadsCreate(input: $input) {
//                    stagedTargets {
//                      url
//                      parameters {
//                        name
//                        value
//                      }
//                    }
//                  }
//                }
//            """);
//            jsonRequest.put("variables", new JSONObject().put("input", new JSONObject()
//                    .put("filename", customFilename)
//                    .put("resource", "FILE")
//                    .put("fileSize", fileContent.length)
//                    .put("mimeType",  detectMimeType(s3Url))
//            ));
//
//            HttpEntity<String> requestEntity = new HttpEntity<>(jsonRequest.toString(), headers);
//            logger.info("Calling Shopify GraphQL API");
//
//            ResponseEntity<String> response = restTemplate.exchange(
//                    SHOPIFY_APP_UPLOAD_URL, HttpMethod.POST, requestEntity, String.class);
//
//            logger.info("Response received from Shopify: {}", response.getBody());
//            return response.getBody();  // Should contain upload URL
//        } catch (Exception e) {
//            logger.error("Failed to upload image to Shopify", e);
//            return null;
//        }
//    }
}

//class ImageUploadRequest {
//    private String s3Url;
//    private String customFilename;
//
//    public ImageUploadRequest(String s3Url, String customFilename) {
//        this.s3Url = s3Url;
//        this.customFilename = customFilename;
//    }
//
//    public String getS3Url() {
//        return s3Url;
//    }
//
//    public String getCustomFilename() {
//        return customFilename;
//    }
//}


