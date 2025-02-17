package com.abelini_s3_migrate;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/shopify/media")
public class MediaController {

    private static final Set<String> SUPPORTED_MIME_TYPES = Set.of(
            "image/png", "image/jpeg", "image/gif", "video/mp4", "video/quicktime", "model/gltf-binary", "model/vnd.usdz+zip"
    );
    @Value("${shopify_store}")
    private String shopifyStore;

    @Value("${shopify_access_token}")
    private String SHOPIFY_ACCESS_TOKEN;
    private  final String SHOPIFY_GRAPHQL_URL = shopifyStore + "/admin/api/2024-04/graphql.json";
    private final RestTemplate restTemplate = new RestTemplate();
    private final Tika tika = new Tika();
    private static final Logger logger = LoggerFactory.getLogger(ShopifyService.class);

    @PostMapping("/upload")
    public ResponseEntity<String> uploadMedia(@RequestParam(required = false, name = "filePath") String filePath) throws IOException, CsvException {
        logger.info("1");
        if (filePath == null) {
            logger.info("2");
            filePath = "src/main/resources/s3file/s3_url_list.csv";
        }
        logger.info("3");
        List<String> csvData = readCSV(filePath);
        logger.info("5");
        logger.info("data size :::"+ csvData.size());
        List<String> remainingData = new ArrayList<>();
        remainingData.add("image_url");
        int successCount = 0;
        logger.info("6");
        for (String row : csvData) {
            logger.info("for loop start");
            String fileName = generateShopifyFilePath(row); ;

            try {
                URL url = new URL(row);
                String mimeType = tika.detect(url);
                long fileSize = url.openConnection().getContentLengthLong();

                if (!SUPPORTED_MIME_TYPES.contains(mimeType)) {
                    remainingData.add(row);
                    logger.info("7");
                    continue;
                }

                if (uploadToShopify(row, fileName, mimeType, fileSize)) {
                    successCount++;
                    logger.info("8");
                } else {
                    logger.info("9");
                    remainingData.add(row);
                }
            } catch (Exception e) {
                remainingData.add(row);
                logger.info("10");
                e.printStackTrace();
            }
        }
        logger.info("for loop ends");

        updateCSV(filePath, remainingData);
        return ResponseEntity.ok(successCount + " files successfully uploaded.");
    }

    private List<String> readCSV(String filePath) throws IOException, CsvException {
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            List<String[]> records = reader.readAll();
            logger.info("4");
            return records.stream().skip(1) // Skip header row
                    .map(row -> row[0])
                    .collect(Collectors.toList());
        }
    }

    private String generateShopifyFilePath(String fileUrl) {
        String relativePath = fileUrl.substring(fileUrl.indexOf(".com/") + 5).replace("/", "_");
        String fileName = Pattern.compile("\\s+").matcher(relativePath).replaceAll("_");
        logger.info("file name ::: {}", fileName);
        return fileName;
    }

    private void updateCSV(String filePath, List<String> remainingData) {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath))) {
            for (String row : remainingData) {
                writer.write(String.join(",", row));
                writer.newLine();
                logger.info("11");
            }
        } catch (IOException e) {
            logger.info("11-error");
            e.printStackTrace();
        }
    }

    private boolean uploadToShopify(String s3Url, String fileName, String mimeType, long fileSize) {
        String query = """
                mutation generateStagedUploads($input: [StagedUploadInput!]!) {
                  stagedUploadsCreate(input: $input) {
                    stagedTargets {
                      url
                      resourceUrl
                      parameters {
                        name
                        value
                      }
                    }
                    userErrors {
                      field
                      message
                    }
                  }
                }
                """;

        Map<String, Object> variables = Map.of("input", List.of(Map.of(
                "filename", fileName,
                "mimeType", mimeType,
                "resource", getResourceType(mimeType),
                "fileSize", fileSize
        )));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.valueOf("application/json"));
        headers.set("X-Shopify-Access-Token", SHOPIFY_ACCESS_TOKEN);
        logger.info("headers ::: " + headers);
        Map<String, Object> requestBody = Map.of("query", query, "variables", variables);
        HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestBody, headers);

        ResponseEntity<Map> response = restTemplate.exchange(SHOPIFY_GRAPHQL_URL, HttpMethod.POST, request, Map.class);
        logger.info("7");
        return response.getStatusCode() == HttpStatus.OK;
    }

    private String getResourceType(String mimeType) {
        return switch (mimeType) {
            case "image/png", "image/jpeg", "image/gif" -> "IMAGE";
            case "video/mp4", "video/quicktime" -> "VIDEO";
            case "model/gltf-binary", "model/vnd.usdz+zip" -> "MODEL_3D";
            default -> "UNKNOWN";
        };
    }
}
