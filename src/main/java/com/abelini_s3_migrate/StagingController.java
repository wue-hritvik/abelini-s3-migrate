package com.abelini_s3_migrate;

import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@RestController
@RequestMapping("/api/staging")
public class StagingController {
    @Value("${shopify.store}")
    private String shopifyStore;

    @Value("${shopify.access.token}")
    private String ACCESS_TOKEN;
    private static final Logger logger = LoggerFactory.getLogger(StagingController.class);
    private final String SHOPIFY_GRAPHQL_URL = shopifyStore + "/admin/api/2024-04/graphql.json";
    private final ShopifyService shopifyService;

    public StagingController(ShopifyService shopifyService) {
        this.shopifyService = shopifyService;
    }

    // ✅ Generate Shopify File Path
    private String generateShopifyFilePath(String fileUrl) {
        String relativePath = fileUrl.substring(fileUrl.indexOf(".com/") + 5).replace("/", "_");
        String fileName = Pattern.compile("\\s+").matcher(relativePath).replaceAll("_");
        logger.info("Generated file name: {}", fileName);
        return fileName;
    }

    @PostMapping("/upload")
    public ResponseEntity<String> uploadFileFromS3(@RequestParam String s3Url) {
        try {
            String customFileName = generateShopifyFilePath(s3Url);

            // ✅ 1️⃣ Download File from S3
            byte[] fileBytes = downloadFile(s3Url);
            String mimeType = detectMimeType(fileBytes);
            long fileSize = fileBytes.length;

            // ✅ 2️⃣ Append Extension if Missing
            String finalFileName = ensureFileExtension(customFileName, mimeType);

            // ✅ 3️⃣ Get Shopify Staged Upload URL
            Map<String, Object> uploadDetails = getShopifyUploadUrl(finalFileName, mimeType, fileSize);
            logger.info("upload details ::: {}", uploadDetails);

            // ✅ Extract `stagedTargets` list from uploadDetails
            List<Map<String, Object>> stagedTargets = (List<Map<String, Object>>) uploadDetails.get("stagedTargets");

            // ✅ Extract `resourceUrl` from the first item in the list
            String resourceUrl = (String) stagedTargets.get(0).get("resourceUrl");

            logger.info("Extracted resourceUrl: {}", resourceUrl);

            // ✅ 4️⃣ Upload File to Shopify Storage
            uploadFileToShopifyStorage(fileBytes, uploadDetails, finalFileName);

            List<String> urls = new ArrayList<>();
            urls.add(resourceUrl);
            // ✅ 6️⃣ Register the File in Shopify
            shopifyService.registerBatchInShopify(urls);

            return ResponseEntity.ok("File uploaded successfully: " + finalFileName);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage());
        }
    }

    // ✅ Download File from S3
    private byte[] downloadFile(String fileUrl) throws Exception {
        RestTemplate restTemplate = new RestTemplate();
        return restTemplate.getForObject(fileUrl, byte[].class);
    }

    // ✅ Detect MIME Type
    private String detectMimeType(byte[] fileBytes) throws Exception {
        Tika tika = new Tika();
        return tika.detect(new ByteArrayInputStream(fileBytes));
    }

    // ✅ Ensure Filename Has Correct Extension
    private String ensureFileExtension(String customFileName, String mimeType) {
        String extension = mimeType.split("/")[1]; // Extract extension from MIME type
        if (!customFileName.contains(".")) {
            return customFileName + "." + extension;
        }
        return customFileName;
    }

    // ✅ Get Staged Upload URL from Shopify
    private Map<String, Object> getShopifyUploadUrl(String filename, String mimeType, long fileSize) {
        RestTemplate restTemplate = new RestTemplate();

        String graphqlQuery = String.format("""
                    mutation {
                      stagedUploadsCreate(input: {
                        fileSize: "%d",
                        filename: "%s",
                        mimeType: "%s",
                        resource: FILE
                      }) {
                        stagedTargets {
                          url
                          resourceUrl
                          parameters {
                            name
                            value
                          }
                        }
                      }
                    }
                """, fileSize, filename, mimeType);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Shopify-Access-Token", ACCESS_TOKEN);

        Map<String, String> body = Map.of("query", graphqlQuery);
        HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);

        ResponseEntity<Map> response = restTemplate.exchange(SHOPIFY_GRAPHQL_URL, HttpMethod.POST, request, Map.class);
        logger.info("Shopify API Response: {}", response.getBody());

        return (Map<String, Object>) ((Map<String, Object>) response.getBody().get("data")).get("stagedUploadsCreate");
    }

    // ✅ Extract File Key for Registration
    private String extractFileKey(String uploadUrl) {
        try {
            // ✅ Extract the portion after "/files/"
            return uploadUrl.split("/files/")[1].split("\\?")[0];
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract file key from URL: " + uploadUrl);
        }
    }

    // ✅ Upload File to Shopify Storage
    private void uploadFileToShopifyStorage(byte[] fileBytes, Map<String, Object> uploadDetails, String filename) throws Exception {
        RestTemplate restTemplate = new RestTemplate();

        // ✅ Extract Upload URL
        List<Map<String, Object>> stagedTargets = (List<Map<String, Object>>) uploadDetails.get("stagedTargets");
        String uploadUrl = (String) stagedTargets.get(0).get("url");
        logger.info("url :::{}", uploadUrl);

        // ✅ Extract required parameters
        List<Map<String, String>> parameters = (List<Map<String, String>>) stagedTargets.get(0).get("parameters");
        logger.info("parameters :::{}", parameters);
        // ✅ Prepare Form Data
        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        for (Map<String, String> param : parameters) {
            body.add(param.get("name"), param.get("value"));
        }

        // ✅ Attach File as ByteArrayResource
        body.add("file", fileBytes);

        // ✅ Set Headers (No Authentication Headers)
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);

        logger.info("request entity :::{}", requestEntity);
        // ✅ Send Request
        ResponseEntity<String> response = restTemplate.exchange(uploadUrl, HttpMethod.PUT, requestEntity, String.class);

        if (response.getStatusCode().is2xxSuccessful()) {
            logger.info("✅ Shopify Upload Successful! HTTP Status: {} :: response :::{}", response.getStatusCode(), response.getBody());
        } else {
            logger.error("❌ Shopify Upload Failed! HTTP Status: {}", response.getStatusCode());
        }
    }


    // ✅ Register File in Shopify
    private void registerFileInShopify(String fileKey, String mimeType, String filename) {
        RestTemplate restTemplate = new RestTemplate();

        // ✅ GraphQL Mutation for File Registration
        String graphqlQuery = String.format("""
                    mutation {
                      fileCreate(files: [{
                        originalSource: "%s",
                        contentType: %s,
                        filename: "%s",
                        alt: "%s"
                      }]) {
                        files {
                          id
                          fileStatus  // ✅ Check status (READY or FAILED)
                          filename
                          alt
                        }
                      }
                    }
                """, fileKey, getShopifyFileType(mimeType), filename, filename);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Shopify-Access-Token", ACCESS_TOKEN);

        Map<String, String> body = Map.of("query", graphqlQuery);
        HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);

        // ✅ Send Request to Shopify
        ResponseEntity<Map> response = restTemplate.exchange(
                SHOPIFY_GRAPHQL_URL, HttpMethod.POST, request, Map.class);
        logger.info("Shopify File Registration Response: {}", response.getBody());
        if (response.getStatusCode().is2xxSuccessful() && !response.getBody().containsKey("errors")) {
            logger.info("success");
        } else {
            logger.error("upload failed");
        }

    }

    private String getShopifyFileType(String mimeType) {
        if (mimeType.startsWith("image/")) return "IMAGE";
        if (mimeType.startsWith("video/")) return "VIDEO";
        return "FILE";
    }
}
