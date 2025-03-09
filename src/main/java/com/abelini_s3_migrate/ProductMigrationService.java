package com.abelini_s3_migrate;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class ProductMigrationService {

    private static final Logger logger = LoggerFactory.getLogger(ProductMigrationService.class);
    @Value("${shopify_store}")
    private String shopifyStore;

    @Value("${shopify_access_token}")
    private String accessToken;

    private final Gson gson = new Gson();
    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final HttpClient client = HttpClient.newHttpClient();
    private static final int MAX_CONCURRENT_BATCHES = 5;
    private static final Semaphore semaphore = new Semaphore(MAX_CONCURRENT_BATCHES);
    private static final int API_COST_PER_CALL = 40;
    private static final int MAX_POINTS = 19000;
    private static final int RECOVERY_RATE = 1000;
    private static final int SAFE_THRESHOLD = 1000;
    private static final AtomicInteger remainingPoints = new AtomicInteger(MAX_POINTS);

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

    public List<String> readCSV(MultipartFile file) throws IOException, CsvException {
        try (CSVReader reader = new CSVReader(new InputStreamReader(file.getInputStream()))) {
            List<String[]> records = reader.readAll();
            return records.stream()
                    .skip(1) // Skip header row
                    .map(row -> row[0]) // Assuming product IDs are in the first column
                    .collect(Collectors.toList());
        }
    }

    private final String GRAPHQL_QUERY_METAOBJECT = """
                query GetAllMetaobjects($type: String!) {
                  metaobjects(type: $type, first: 250) {
                    edges {
                      node {
                        id
                        field(key: "name") {
                          value
                        }
                      }
                    }
                  }
                }
            """;

    private final String GRAPHQL_QUERY_PRODUCTS_CREATE = """
                mutation ProductCreateWithMetafields($product: ProductCreateInput!) {
                    productCreate(product: $product) {
                        product {
                            id
                            title
                        }
                        userErrors {
                            field
                            message
                        }
                    }
                }
            """;


    private Map<String, String> fetchMetaobjectDetails(String type) {
        Map<String, String> result = new HashMap<>();
        try {
            regulateApiRate();
            remainingPoints.addAndGet(-API_COST_PER_CALL);
            // Prepare GraphQL payload
            Map<String, Object> variables = Map.of("type", type);

            // Make the Shopify API call
            String response = sendGraphQLRequest(GRAPHQL_QUERY_METAOBJECT, objectMapper.writeValueAsString(variables), false);

            if (response == null) {
                logger.error("error while fetching meta object details type :: {}", type);
                return result;
            }

            // Parse JSON response
            JsonNode rootNode = objectMapper.readTree(response);
            JsonNode edges = rootNode.path("data").path("metaobjects").path("edges");

            for (JsonNode edge : edges) {
                String id = edge.path("node").path("id").asText();
                String name = edge.path("node").path("field").path("value").asText();
                if (!name.isEmpty() && !id.isEmpty()) {
                    result.put(name, id);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.info("error in fetching meta object type:: {}", type);
        }
        return result;
    }

    private String sendGraphQLRequest(String query, String variables, boolean is24) {
        try {
            String url = "";
            if (is24) {
                url = shopifyStore + "/admin/api/2024-04/graphql.json";
            } else {
                url = shopifyStore + "/admin/api/2025-01/graphql.json";
            }

            HttpHeaders headers = new HttpHeaders();
            headers.set("X-Shopify-Access-Token", accessToken.trim());
            headers.set("Content-Type", "application/json");

            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("query", query);
            requestBody.put("variables", objectMapper.readValue(variables, Map.class));

            String requestJson = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(requestBody);

            // Log the full HTTP Request (excluding sensitive access token)
            logger.info("=== Shopify HTTP Request ===");
            logger.info("POST {}", url);
            logger.info("Headers: {{ X-Shopify-Access-Token: [REDACTED], Content-Type: application/json }}");
            logger.info("Body: {}", requestJson);

            HttpEntity<String> request = new HttpEntity<>(requestJson, headers);
            ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);

            // Extract X-Request-Id header
            String requestId = response.getHeaders().getFirst("X-Request-Id");

            // Log the full HTTP Response
            String body = response.getBody();
            logger.info("=== Shopify HTTP Response ===");
            logger.info("Status: {}", response.getStatusCode());
//            logger.info("Headers: {}", response.getHeaders());
            logger.info("X-Request-Id: {}", requestId);
            logger.info("Body: {}", body);

            if (body.contains("\"userErrors\":[")) {
                if (!body.contains("\"userErrors\":[]")) {
                    logger.error("Error receive in shopify response: {}", response);
                    return null;
                }
            } else if (body.contains("\"errors\":[")) {
                if (!body.contains("\"errors\":[]")) {
                    logger.error("Error uploading batch: {}", response);
                }
                logger.error("Error receive in shopify response: {}", response);
                return null;
            }

            return body;
        } catch (
                Exception e) {
            logger.error("Error sending GraphQL request: {}", e.getMessage(), e);
            return null;
        }
    }

    // Methods for each metaobject type
    public Map<String, String> getAllMetalsDetails() {
        return fetchMetaobjectDetails("metal");
    }

    public Map<String, String> getAllBackingDetails() {
        return fetchMetaobjectDetails("backing");
    }

    public Map<String, String> getAllRecipientDetails() {
        return fetchMetaobjectDetails("by_recipient");
    }

    public Map<String, String> getAllSettingTypeDetails() {
        return fetchMetaobjectDetails("setting_type");
    }

    public Map<String, String> getAllShapeDetails() {
        return fetchMetaobjectDetails("shape");
    }

    public Map<String, String> getAllStoneTypeDetails() {
        return fetchMetaobjectDetails("stone_type");
    }

    private static final Map<String, String> metalMap = new ConcurrentHashMap<>();
    private static final Map<String, String> stoneTypeMap = new ConcurrentHashMap<>();
    private static final Map<String, String> shapeMap = new ConcurrentHashMap<>();
    private static final Map<String, String> settingTypeMap = new ConcurrentHashMap<>();
    private static final Map<String, String> recipientMap = new ConcurrentHashMap<>();
    private static final Map<String, String> backingMap = new ConcurrentHashMap<>();
    private static final AtomicBoolean initialized = new AtomicBoolean(false);

    @PostConstruct
    public void init() {
        initializeStaticData();
    }

    public synchronized void initializeStaticData() {
        if (initialized.compareAndSet(false, true)) {
            metalMap.putAll(getAllMetalsDetails());
            logger.info("Loaded all metal :: {}", metalMap);

            stoneTypeMap.putAll(getAllStoneTypeDetails());
            logger.info("Loaded all stone type :: {}", stoneTypeMap);

            shapeMap.putAll(getAllShapeDetails());
            logger.info("Loaded all shape :: {}", shapeMap);

            settingTypeMap.putAll(getAllSettingTypeDetails());
            logger.info("Loaded all setting type :: {}", settingTypeMap);

            recipientMap.putAll(getAllRecipientDetails());
            logger.info("Loaded all recipient :: {}", recipientMap);

            backingMap.putAll(getAllBackingDetails());
            logger.info("Loaded all backing :: {}", backingMap);
        }
    }

    private final AtomicInteger totalProcessed = new AtomicInteger(0);
    private final AtomicInteger totalSuccess = new AtomicInteger(0);
    private final AtomicInteger totalFailed = new AtomicInteger(0);

    @Async
    public void processProducts(MultipartFile file, String singleId) {
        try {
            logger.info("Starting product import... at:: {}", ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z")));
            List<String> ids = new ArrayList<>();

            if (!singleId.equals("0")) {
                ids.add(singleId);
            } else {
                if (file == null || file.isEmpty()) {
                    logger.error("No file uploaded. Skipping processing.");
                    return;
                }
                ids = readCSV(file);
            }

            if (ids.isEmpty()) {
                logger.error("No product ids found. Skipping processing.");
                return;
            }
            long totalCount = ids.size();
            logger.info("total count :: {}", totalCount);
            for (String id : ids) {

                logger.info("Processing product id: " + id);
                JSONObject apiResponse = fetchProductDetailsFromApi(id);

                if (apiResponse == null || apiResponse.isEmpty()) {
                    logger.error("error while creating product id: " + id);
                    apiResponse = null;
                    totalProcessed.incrementAndGet();
                    totalFailed.incrementAndGet();
                    logger.info("processed product id: {}, with status :: {} , processed till now :: {}/{}", id, apiResponse != null, totalProcessed.get(), totalCount);
                    continue;
                }

                Map<String, Object> data = processResponse(apiResponse);

//                String mutation = buildProductCreateMutation(data);

                regulateApiRate();
                remainingPoints.addAndGet(-API_COST_PER_CALL);

                Map<String, Object> product = new HashMap<>();
                product.put("product", data);
                String response = sendGraphQLRequest(GRAPHQL_QUERY_PRODUCTS_CREATE, objectMapper.writeValueAsString(product), false);

                if (response == null) {
                    logger.error("error while creating product id: " + id);
                    totalProcessed.incrementAndGet();
                    totalFailed.incrementAndGet();
                    logger.info("processed product id: {}, with status :: {} , processed till now :: {}/{}", id, false, totalProcessed.get(), totalCount);
                    continue;
                }

                Map<String, String> extratcIds = extractProductIdAndVariendId(response);

                getBaseVarientAndSetSkuAndPrice(extratcIds.get("product"), apiResponse);

                List<JSONObject> metaFields = processMetafields(apiResponse);

                processApiResponseAndUploadMetafields(extratcIds.get("product"), metaFields);

                totalProcessed.incrementAndGet();
                logger.info("Product created successfully for product id: " + id);
                totalSuccess.incrementAndGet();

                logger.info("processed product id: {}, with status :: {} , processed till now :: {}/{}", id, response != null, totalProcessed.get(), totalCount);
            }

            logger.info("Import process complete with total processed :: {}/{} with success: {}, failed: {} and ended at :: {}", totalProcessed.get(), totalCount, totalSuccess.get(), totalFailed.get(), ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z")));

        } catch (Exception e) {
            logger.error("error in process product :: {}", e.getMessage(), e);
        }
    }

    private void getBaseVarientAndSetSkuAndPrice(String productId, JSONObject apiResponse) {
        try {
            // Step 1: Fetch Base Variant ID
            String query = """
                        query GetBaseVariant($productId: ID!) {
                            product(id: $productId) {
                                variants(first: 1) {
                                    edges {
                                        node {
                                            id
                                            sku
                                            price
                                        }
                                    }
                                }
                            }
                        }
                    """;

            Map<String, Object> variable = new HashMap<>();
            variable.put("productId", productId);

            String response = sendGraphQLRequest(query, objectMapper.writeValueAsString(variable), false);
            if (response == null) {
                logger.error("Failed to fetch base variant for product ID: {}", productId);
                return;
            }

            JSONObject responseObj = new JSONObject(response);

            // Extract base variant ID
            JSONObject data = responseObj.optJSONObject("data");
            if (data == null) {
                logger.error("Invalid GraphQL response: 'data' is missing");
                return;
            }

            JSONObject product = data.optJSONObject("product");
            if (product == null) {
                logger.warn("No product found for ID: {}", productId);
                return;
            }

            JSONObject variants = product.optJSONObject("variants");
            if (variants == null) {
                logger.warn("No variants found for product ID: {}", productId);
                return;
            }

            JSONArray edges = variants.optJSONArray("edges");
            if (edges == null || edges.isEmpty()) {
                logger.warn("No variant edges found for product ID: {}", productId);
                return;
            }

            // Extract base variant
            JSONObject baseVariant = edges.getJSONObject(0).optJSONObject("node");
            if (baseVariant == null) {
                logger.warn("Base variant node is missing for product ID: {}", productId);
                return;
            }

            String variantId = baseVariant.optString("id", null);
            if (variantId == null) {
                logger.warn("Variant ID is missing for product ID: {}", productId);
                return;
            }

            Map<String, Object> variables = new HashMap<>();
            variables.put("variantId", variantId);

            if (apiResponse.has("sku")) {
                variables.put("sku", apiResponse.optString("sku"));
            }

            if (apiResponse.has("price")) {
                String priceStr = String.format("%.2f", apiResponse.optDouble("price", 0.0)); // Ensure 2 decimal places
                variables.put("price", new BigDecimal(priceStr));
            }

            // Correct Mutation
            String mutation = """
                    mutation UpdateVariant($variantId: ID!, $sku: String, $price: Money) {
                      productVariantUpdate(input: { id: $variantId, sku: $sku, price: $price }) {
                        productVariant {
                          id
                          sku
                          price
                        }
                        userErrors {
                          field
                          message
                        }
                      }
                    }
                    """;

            String updateResponse = sendGraphQLRequest(mutation, objectMapper.writeValueAsString(variables), true);
            if (updateResponse == null) {
                logger.error("Failed to update variant ID: {}", variantId);
            }

            logger.info("varient update successfully for product id: " + productId);

        } catch (Exception e) {
            logger.error("Error processing base variant update: {}", e.getMessage(), e);
        }
    }

    private Map<String, String> extractProductIdAndVariendId(String response) throws JsonProcessingException {
        // Parse JSON response
        Map<String, String> result = new HashMap<>();
        JsonNode rootNode = objectMapper.readTree(response);

        // Extract product ID
        JsonNode productNode = rootNode.path("data").path("productCreate").path("product").path("id");
        if (productNode.isMissingNode()) {
            System.out.println("Error: Product ID not found in API response.");
            return Collections.emptyMap();
        }
        String productId = productNode.asText();
        System.out.println("Extracted Product ID: " + productId);
        result.put("product", productId);

        JsonNode vairent = rootNode.path("data").path("productCreate")
                .path("product").path("variants").path("edges")
                .path("node").path("id");
        if (!vairent.isMissingNode()) {
            String va = vairent.asText();
            result.put("varient", va);
            System.out.println("Extracted varient ID: " + va);
        }
        return result;
    }

    public void processApiResponseAndUploadMetafields(String productId, List<JSONObject> metaFields) {
        try {
            // Add metafields one by one
            for (JSONObject metafield : metaFields) {
                regulateApiRate();
                remainingPoints.addAndGet(-API_COST_PER_CALL);
                logger.info("uploading meta field :: {}", metafield.get("key"));
                addMetafieldToProduct(productId, metafield);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void addMetafieldToProduct(String productId, JSONObject metafield) throws JsonProcessingException {
        String namespace = metafield.get("namespace").toString();
        String key = metafield.get("key").toString();
        String type = metafield.get("type").toString();
        var value = metafield.get("value");

        if (value == null || StringUtils.isBlank(value.toString()) || "[]".contains(value.toString())) {
            return;
        }


        if (type.contains("text_field") || type.contains("number")) {
            if (value instanceof List) {
                logger.info("instance of list");
                // Convert list to a comma-separated string
                value = ((List<?>) value).stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(","));
            } else {
                value = value.toString().replace("[", "").replace("]", "").replace("\"", "");  // Remove brackets if present
            }
            value = "\"" + value + "\""; // Ensure it's treated as a string in GraphQL
        } else if (type.contains("list")) {
            // Convert to a valid JSON array format if it's a list
            if (value instanceof List) {
                logger.info("list value ::: {}", value);
                value = objectMapper.writeValueAsString(value);
            }
            value = objectMapper.writeValueAsString(value);
        } else if (type.contains("json")) {
            value = objectMapper.writeValueAsString(value.toString());
        }

        // Construct GraphQL mutation for adding a metafield
        String graphqlMutation = String.format("""
                mutation {
                  metafieldsSet(metafields: [{
                    ownerId: "%s",
                    namespace: "%s",
                    key: "%s",
                    type: "%s",
                    value: %s
                  }]) {
                    metafields {
                      id
                      key
                      value
                    }
                    userErrors {
                      field
                      message
                    }
                  }
                }
                """, productId, namespace, key, type, value);

        // Execute the GraphQL request
        executeGraphQLRequest(graphqlMutation);
    }

    private String executeGraphQLRequest(String graphqlQuery) {
        try {
            // Escape GraphQL query properly
            String requestBody = objectMapper.writeValueAsString(Map.of("query", graphqlQuery));
            logger.info("=== Shopify GraphQL Request ===");
            logger.info("Body: {}", requestBody);
            String url = shopifyStore + "/admin/api/2025-01/graphql.json";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .header("X-Shopify-Access-Token", accessToken)
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            int statusCode = response.statusCode();
            String responseBody = response.body();
            String requestId = response.headers().firstValue("X-Request-Id").orElse("N/A");

            // Log response
            logger.info("=== Shopify HTTP Response ===");
            logger.info("Status: {}", statusCode);
            logger.info("X-Request-Id: {}", requestId);
            logger.info("Body: {}", responseBody);

            // Parse JSON response
            JsonNode responseJson = objectMapper.readTree(responseBody);

            // Check for errors
            if (responseJson.has("errors") && !responseJson.get("errors").isEmpty()) {
                logger.error("GraphQL request error: {}", responseJson.get("errors"));
                return null;
            }

            // Check for userErrors in response data
            if (responseJson.has("data")) {
                JsonNode dataNode = responseJson.get("data");
                if (dataNode.has("userErrors") && !dataNode.get("userErrors").isEmpty()) {
                    logger.error("User Errors in Shopify response: {}", dataNode.get("userErrors"));
                    return null;
                }
            }

            return responseBody;

        } catch (Exception e) {
            logger.error("Exception during GraphQL request: ", e);
            return null;
        }
    }


//    public String buildProductCreateMutation(JSONObject apiResponse) {
//
//        try {
//            ObjectNode productNode = objectMapper.createObjectNode();
//
//            if (apiResponse.has("title")) {
//                productNode.put("title", apiResponse.get("title").toString());
//            }
//            if (apiResponse.has("descriptionHtml")) {
//                productNode.put("descriptionHtml", apiResponse.get("descriptionHtml").toString());
//            }
//            if (apiResponse.has("tags")) {
//                productNode.put("tags", apiResponse.get("tags").toString());
//            }
//            if (apiResponse.has("sku")) {
//                productNode.put("sku", apiResponse.get("sku").toString());
//            }
//            if (apiResponse.has("price")) {
//                productNode.put("price", apiResponse.get("price").toString());
//            }
//
//            // Variants
//            if (apiResponse.has("sku")) {
//                ObjectNode variantNode = objectMapper.createObjectNode();
//                variantNode.put("sku", apiResponse.get("sku").toString());
//                productNode.set("variants", objectMapper.createArrayNode().add(variantNode));
//            }
//
//            String productJson = objectMapper.writeValueAsString(productNode);
//            logger.info("formatted product json :: {}", productJson);
//
//            return """
//                    mutation ProductCreateWithMetafields {
//                      productCreate(
//                        product: %s
//                      ) {
//                        product {
//                          id
//                          title
//                        }
//                        userErrors {
//                          field
//                          message
//                        }
//                      }
//                    }
//                    """.formatted(productJson);
//
//        } catch (Exception e) {
//            throw new RuntimeException("Error building GraphQL mutation", e);
//        }
//    }

    private JSONObject fetchProductDetailsFromApi(String productId) {
        String url = "https://www.abelini.com/shopify/api/product/product_detail.php";
        Map<String, String> request = new HashMap<>();
        request.put("product_id", productId);

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        HttpEntity<Map<String, String>> entity = new HttpEntity<>(request, headers);

        String response = restTemplate.postForObject(url, entity, String.class);
//        logger.info("api response :: {}", response);
        try {
            JSONArray jsonArray = new JSONArray(response);
            if (!jsonArray.isEmpty()) {
                return jsonArray.getJSONObject(0);
            } else {
                logger.error("API returned an empty list for product ID: {}", productId);
                return null;
            }
        } catch (Exception e) {
            System.out.println("Failed to parse product details JSON: " + e.getMessage());
            return null;
        }
    }

    private Map<String, Object> processResponse(JSONObject apiResponse) {
        Map<String, Object> response = new HashMap<>();
        if (apiResponse == null) {
            logger.error("API response is null. Skipping processing.");
            return null;
        }

        // Add only if present
        if (apiResponse.has("name")) {
            response.put("title", apiResponse.optString("name"));
        }

        if (apiResponse.has("description")) {
            response.put("descriptionHtml", apiResponse.optString("description"));
        }

        response.put("vendor", "Abelini Ltd.");

        if (apiResponse.has("tag")) {
            response.put("tags", apiResponse.optString("tag"));
        }

//        if (apiResponse.has("sku")) {
//            response.put("sku", apiResponse.optString("sku"));
//        }

//        if (apiResponse.has("price")) {
//            response.put("price", apiResponse.optDouble("price", 0.0));
//        }

        // Variants
//        if (apiResponse.has("sku")) {
//            Map<String, Object> variant = new HashMap<>();
//            variant.put("sku", apiResponse.optString("sku"));
//            if (apiResponse.has("price")) {
//                response.put("price", apiResponse.optDouble("price", 0.0));
//            }
//            response.put("variants", Collections.singletonList(variant));
//        }

        return response;
    }

    private List<JSONObject> processMetafields(JSONObject rawMetafields) throws JsonProcessingException {
        List<JSONObject> processedMetafields = new ArrayList<>();

        addMetafield(processedMetafields, rawMetafields, "having_down_360_single", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "having_front_360_single", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "having_stone_shape_single", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "having_stone_type_single", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "having_modal_image", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "having_image_view", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "having_carat", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "having_down_360", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "having_front_360", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "default_view", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "filter", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "upc", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "product_type_id", "number_integer");
        addMetafield(processedMetafields, rawMetafields, "diamond_selection", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "view_360", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "product_price_str", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "having_modal_image_single", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "having_image_view_single", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "having_carat_single", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "image_counter", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "product_minimum_price", "json", "minimum_price_json");
        addMetafield(processedMetafields, rawMetafields, "product_id", "number_integer", "open_cart_product_id");
        addMetafield(processedMetafields, rawMetafields, "product_options", "json", "option_json");

        // Special metafields requiring processing
        addProcessedMetafield(processedMetafields, rawMetafields, "product_options", "list.metaobject_reference", "metal", this::getMetalIds);
        addProcessedMetafield(processedMetafields, rawMetafields, "product_options", "list.metaobject_reference", "stone_type", this::getStoneTypeIds);
        addProcessedMetafield(processedMetafields, rawMetafields, "product_options", "list.metaobject_reference", "shape", this::getShapeIds);
        addProcessedMetafield(processedMetafields, rawMetafields, "product_options", "list.metaobject_reference", "backing", this::getBackingIds);
//        addProcessedMetafield(processedMetafields, rawMetafields, "product_options", "list.metaobject_reference", "carat", this::getCaratIds);
        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "list.metaobject_reference", "setting_type", this::getSettingTypeIds);
        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "list.metaobject_reference", "by_recipient", this::getRecipientIds);

        return processedMetafields;
    }

    private void addMetafield(List<JSONObject> metafields, JSONObject rawMetafields, String key, String type) {
        addMetafield(metafields, rawMetafields, key, type, key);
    }

    private void addMetafield(List<JSONObject> metafields, JSONObject rawMetafields, String key, String type, String metafieldKey) {
        logger.info("checking for meta fields :: {}", key);
        if (rawMetafields.has(key) && !rawMetafields.isNull(key)) {
            logger.info("checking success for meta fields :: {}", key);
            JSONObject metafield = new JSONObject();
            metafield.put("namespace", "custom");
            metafield.put("key", metafieldKey);
            metafield.put("type", type);
            metafield.put("value", rawMetafields.get(key));
            metafields.add(metafield);
        }
    }

    private void addProcessedMetafield(List<JSONObject> metafields, JSONObject rawMetafields, String key, String type, String metafieldKey, Function<Object, Object> processor) throws JsonProcessingException {
        logger.info("checking for processed meta fields :: {}", key);
        if (rawMetafields.has(key) && !rawMetafields.isNull(key)) {
            logger.info("checking for success processed meta fields :: {}", key);
            JSONObject metafield = new JSONObject();
            metafield.put("namespace", "custom");
            metafield.put("key", metafieldKey);
            metafield.put("type", type);
            metafield.put("value", processor.apply(rawMetafields.get(key)));
            metafields.add(metafield);
        }
    }

    private List<String> getRecipientIds(Object productOptions) {
        List<String> names = extractNamesByFilterGroupId(productOptions, "8");
        logger.info("extract recipient :: {}", names);
        List<String> ids = new ArrayList<>();
        for (String name : names) {
            if (recipientMap.containsKey(name)) {
                ids.add(recipientMap.get(name));
            }
        }
        return ids;
    }

    private List<String> getSettingTypeIds(Object productOptions) {
        List<String> names = extractNamesByFilterGroupId(productOptions, "6");
        logger.info("extract setting type :: {}", names);
        List<String> ids = new ArrayList<>();
        for (String name : names) {
            if (settingTypeMap.containsKey(name)) {
                ids.add(settingTypeMap.get(name));
            }
        }
        return ids;
    }

    private List<String> getShapeIds(Object productOptions) {
        try {
            List<String> names = extractOptionNames(productOptions, "shape");
            logger.info("extracted shape names :: {}", names);
            List<String> ids = new ArrayList<>();
            for (String name : names) {
                if (shapeMap.containsKey(name)) {
                    ids.add(shapeMap.get(name));
                }
            }
            return ids;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private List<String> getStoneTypeIds(Object productOptions) {
        try {
            List<String> names = extractOptionNames(productOptions, "stone_type");
            logger.info("extract stone type :: {}", names);
            List<String> ids = new ArrayList<>();
            for (String name : names) {
                if (stoneTypeMap.containsKey(name)) {
                    ids.add(stoneTypeMap.get(name));
                }
            }
            return ids;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private List<String> getMetalIds(Object productOptions) {
        try {
            List<String> names = extractOptionNames(productOptions, "metal");
            logger.info("extract metal :: {}", names);
            List<String> ids = new ArrayList<>();
            for (String name : names) {
                if (metalMap.containsKey(name)) {
                    ids.add(metalMap.get(name));
                }
            }
            return ids;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private Object getBackingIds(Object backing) {
        try {
            logger.info("inside get backing ids");
            List<String> names = extractOptionNames(backing, "backing");
            logger.info("extract backing :: {}", names);
            List<String> ids = new ArrayList<>();
            for (String name : names) {
                if (backingMap.containsKey(name)) {
                    ids.add(backingMap.get(name));
                }
            }
            return ids;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    public List<String> extractBackingNames(Object json) {
        List<String> names = new ArrayList<>();
        logger.info("inside extract backing names");
        try {
            JsonNode rootNode = objectMapper.readTree(json.toString());

            logger.info("Extracting backing names from JSON: {}", rootNode.toPrettyString());

            // Ensure the root node is an array
            if (!rootNode.isArray()) {
                logger.warn("JSON root is not an array. Expected an array of backing objects.");
                return names;
            }

            // Iterate over each backing object
            for (JsonNode backing : rootNode) {
                if (backing.has("product_option_value")) {
                    JsonNode valuesNode = backing.get("product_option_value");

                    if (valuesNode.isObject()) {
                        // Iterate over each nested object (keys like "1", "2", "3")
                        Iterator<JsonNode> elements = valuesNode.elements();
                        while (elements.hasNext()) {
                            JsonNode item = elements.next();
                            if (item.has("name") && !item.get("name").isNull()) {
                                String name = item.get("name").asText();
                                logger.info("Found name: {}", name);
                                names.add(name);
                            }
                        }
                    } else {
                        logger.warn("'product_option_value' is not a JSON object.");
                    }
                } else {
                    logger.warn("'product_option_value' key is missing.");
                }
            }

        } catch (Exception e) {
            logger.error("Error extracting backing names: {}", e.getMessage(), e);
        }

        return names;
    }

    public List<String> extractOptionNames(Object json, String optionKey) {
        List<String> names = new ArrayList<>();

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(json.toString());

            logger.info("Extracting option names for key: {} :: {}", optionKey, rootNode.toPrettyString());

            if (rootNode.has(optionKey)) {
                JsonNode optionNode = rootNode.get(optionKey);

                if (optionNode.has("product_option_value")) {
                    JsonNode valuesNode = optionNode.get("product_option_value");

                    // Iterate over each nested object (keys like "121")
                    for (JsonNode item : valuesNode) {
                        if (item.has("name") && !item.get("name").isNull()) {
                            String name = item.get("name").asText();
                            logger.info("Found name: {}", name);
                            names.add(name);
                        }
                    }
                } else {
                    logger.warn("No 'product_option_value' found inside '{}'", optionKey);
                }
            } else {
                logger.warn("Key '{}' not found in JSON", optionKey);
            }

        } catch (Exception e) {
            logger.error("Error parsing JSON: {}", e.getMessage(), e);
        }

        return names;
    }


    private List<String> extractNamesByFilterGroupId(Object json, String targetFilterGroupId) {
        List<String> names = new ArrayList<>();

        try {
            JsonNode rootNode = objectMapper.readTree(json.toString());

            logger.info("Extracting names for filter_group_id: {} :: {}", targetFilterGroupId, rootNode.toPrettyString());

            // Ensure the root node is an array
            if (!rootNode.isArray()) {
                logger.warn("JSON root is not an array. Expected an array of filter objects.");
                return names;
            }

            // Iterate over each filter object
            for (JsonNode filter : rootNode) {
                if (filter.has("filter_group_id") && filter.has("name")) {
                    String filterGroupId = filter.get("filter_group_id").asText();

                    if (targetFilterGroupId.equals(filterGroupId)) {
                        String name = filter.get("name").asText();
                        logger.info("Found name: {}", name);
                        names.add(name);
                    }
                }
            }

        } catch (Exception e) {
            logger.error("Error parsing JSON: {}", e.getMessage(), e);
        }

        return names;
    }


    private String formatGraphQLValue(Object value) {
        if (value instanceof String) {
            return "\"" + ((String) value).replace("\"", "\\\"") + "\"";
        } else if (value instanceof Collection) {
            // ✅ Convert List to a properly formatted JSON array
            return gson.toJson(value);
        } else {
            return value.toString();
        }
    }


}