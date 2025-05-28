package com.abelini_s3_migrate.service;


import com.abelini_s3_migrate.entity.Product2Lakh;
import com.abelini_s3_migrate.entity.ProductIds;
import com.abelini_s3_migrate.entity.ProductVarientIds;
import com.abelini_s3_migrate.extra.ProductEntry;
import com.abelini_s3_migrate.repo.Product2lakhRepository;
import com.abelini_s3_migrate.repo.ProductIdsRepository;
import com.abelini_s3_migrate.repo.ProductVarientIdsRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@Service
public class ProductMigrationService {

    private static final Logger logger = LoggerFactory.getLogger(ProductMigrationService.class);

    @Value("${shopify_store}")
    private String shopifyStore;

    @Value("${shopify_access_token}")
    private String accessToken;

    @Value("${abelini_jwt_token}")
    private String jwtToken;

    private final ProductIdsRepository productIdsRepository;
    private final ProductVarientIdsRepository productVarientIdsRepository;
    private final Product2lakhRepository product2lakhRepository;

    private final Gson gson = new Gson();
    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final HttpClient client = HttpClient.newHttpClient();
    private static final int MAX_CONCURRENT_BATCHES = 5;
    private static final Semaphore semaphore = new Semaphore(MAX_CONCURRENT_BATCHES);
    private static final int API_COST_PER_CALL = 40;
    private static final int MAX_POINTS = 20000;
    private static final int RECOVERY_RATE = 1000;
    private static final int SAFE_THRESHOLD = 2000;
    private static final AtomicInteger remainingPoints = new AtomicInteger(MAX_POINTS);

    private final ScheduledExecutorService creditRecoveryScheduler = Executors.newScheduledThreadPool(1);

    public ProductMigrationService(ProductIdsRepository productIdsRepository, ProductVarientIdsRepository productVarientIdsRepository, Product2lakhRepository product2lakhRepository) {
        this.productIdsRepository = productIdsRepository;
        this.productVarientIdsRepository = productVarientIdsRepository;
        this.product2lakhRepository = product2lakhRepository;
        creditRecoveryScheduler.scheduleAtFixedRate(() -> {
            int currentPoints = remainingPoints.get();
            if (currentPoints < MAX_POINTS) {
                int newPoints = Math.min(RECOVERY_RATE, MAX_POINTS - currentPoints);
                remainingPoints.addAndGet(newPoints);
                logger.debug("Recovered {} API points. Current points: {}", newPoints, remainingPoints.get());
            }
        }, 1, 1, TimeUnit.SECONDS);
        createFileIfMissing();
    }

    private void regulateApiRate() {
        int maxWaitTime = 10; // Maximum wait time in seconds
        int waitTime = 0;

        while (remainingPoints.get() < SAFE_THRESHOLD) {
            if (waitTime >= maxWaitTime) {
                logger.warn("API points still low after waiting {} seconds. Continuing anyway.", maxWaitTime);
                break;
            }
            logger.info("Low API points ({}), pausing until recovery...", remainingPoints.get());
            try {
                Thread.sleep(1000); // Wait 1 second for recovery
                waitTime++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
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
                           fields {
                             key
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

    private final String GRAPHQL_QUERY_PRODUCTS_UPDATE = """
            mutation productUpdate($input: ProductInput!) {
              productUpdate(input: $input) {
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
                JsonNode fieldsNode = edge.path("node").path("fields");

                // Dynamically extract the first valid field that is NOT `filter_id`
                String key = "";
                String value = "";

                for (JsonNode field : fieldsNode) {
                    key = field.path("key").asText();
                    value = field.path("value").asText();

                    // Ignore null values and `filter_id`
                    if (!value.isEmpty() && !"filter_id".equals(key)) {
                        result.put(value, id);
                        break;  // Store only the first valid key-value pair per metaobject
                    }
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

//    public Map<String, String> getAllBackingDetails() {
//        return fetchMetaobjectDetails("backing");
//    }

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

    private Map<String, String> getAllStyleProductDetails() {
        return fetchMetaobjectDetails("style_product");
    }

    private Map<String, String> getAllBandWidthDetails() {
        return fetchMetaobjectDetails("band_width");
    }

    private Map<String, String> getAllCaratDetails() {
        return fetchMetaobjectDetails("carat");
    }

    private Map<String, String> getAllPersonilizationDetails() {
        return fetchMetaobjectDetails("personalised");
    }

    private Map<String, String> getAllOccasionDetails() {
        return fetchMetaobjectDetails("by_occasion");
    }

    private Map<String, String> getAllRingSizeDetails() {
        return fetchMetaobjectDetails("ring_size");
    }

    private Map<String, String> getAllClarityDetails() {
        return fetchMetaobjectDetails("clarity");
    }

    private Map<String, String> getAllCertificateDetails() {
        return fetchMetaobjectDetails("certificate");
    }

    private Map<String, String> getAllColourDetails() {
        return fetchMetaobjectDetails("colour");
    }

    private Map<String, String> getAllCategoryDetails() {
        return fetchMetaobjectDetails("category");
    }

    private static final Map<String, String> metalMap = new ConcurrentHashMap<>();
    private static final Map<String, String> stoneTypeMap = new ConcurrentHashMap<>();
    private static final Map<String, String> shapeMap = new ConcurrentHashMap<>();
    private static final Map<String, String> settingTypeMap = new ConcurrentHashMap<>();
    private static final Map<String, String> recipientMap = new ConcurrentHashMap<>();
    //    private static final Map<String, String> backingMap = new ConcurrentHashMap<>();
    private static final Map<String, String> categoryMap = new ConcurrentHashMap<>();
    private static final Map<String, String> colourMap = new ConcurrentHashMap<>();
    private static final Map<String, String> certificateMap = new ConcurrentHashMap<>();
    private static final Map<String, String> clarityMap = new ConcurrentHashMap<>();
    private static final Map<String, String> ringSizeMap = new ConcurrentHashMap<>();
    private static final Map<String, String> occasionMap = new ConcurrentHashMap<>();
    private static final Map<String, String> personilizationMap = new ConcurrentHashMap<>();
    private static final Map<String, String> caratMap = new ConcurrentHashMap<>();
    private static final Map<String, String> bandWidthMap = new ConcurrentHashMap<>();
    private static final Map<String, String> styleProductMap = new ConcurrentHashMap<>();
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

//            backingMap.putAll(getAllBackingDetails());
//            logger.info("Loaded all backing :: {}", backingMap);

            categoryMap.putAll(getAllCategoryDetails());
            logger.info("Loaded all category :: {}", categoryMap);

            colourMap.putAll(getAllColourDetails());
            logger.info("Loaded all colour :: {}", colourMap);

            certificateMap.putAll(getAllCertificateDetails());
            logger.info("Loaded all certificate :: {}", certificateMap);

            clarityMap.putAll(getAllClarityDetails());
            logger.info("Loaded all clarity :: {}", clarityMap);

            ringSizeMap.putAll(getAllRingSizeDetails());
            logger.info("Loaded all ring size :: {}", ringSizeMap);

            occasionMap.putAll(getAllOccasionDetails());
            logger.info("Loaded all occasion :: {}", occasionMap);

            personilizationMap.putAll(getAllPersonilizationDetails());
            logger.info("Loaded all personilization :: {}", personilizationMap);

            caratMap.putAll(getAllCaratDetails());
            logger.info("Loaded all carat :: {}", caratMap);

            bandWidthMap.putAll(getAllBandWidthDetails());
            logger.info("Loaded all band width :: {}", bandWidthMap);

            styleProductMap.putAll(getAllStyleProductDetails());
            logger.info("Loaded all style product :: {}", styleProductMap);

        }
    }

    @Async
    public void processProducts(MultipartFile file, String singleId) {
        try {
            String startTime = ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z"));
            logger.info("Starting product import... at:: {}", startTime);
            List<String> ids = new ArrayList<>();
            AtomicInteger totalProcessed = new AtomicInteger(0);
            AtomicInteger totalSuccess = new AtomicInteger(0);
            AtomicInteger totalFailed = new AtomicInteger(0);

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
                totalProcessed.incrementAndGet();
                try {
                    logger.info("Processing product id: " + id);
                    JSONObject apiResponse = fetchProductDetailsFromApi(id);

                    if (apiResponse == null || apiResponse.isEmpty()) {
                        logger.error("null or empty error while creating product id: " + id);
                        totalFailed.incrementAndGet();
                        logger.info("processed product id: {}, with status :: {} , processed till now :: {}/{}", id, false, totalProcessed.get(), totalCount);
                        continue;
                    }

                    Map<String, Object> data = processResponse(apiResponse);
                    regulateApiRate();
                    remainingPoints.addAndGet(-API_COST_PER_CALL);

                    Map<String, Object> product = new HashMap<>();
                    product.put("product", data);
                    String response = sendGraphQLRequest(GRAPHQL_QUERY_PRODUCTS_CREATE, objectMapper.writeValueAsString(product), false);

                    if (response == null) {
                        logger.error("shopify response null error while creating product id: " + id);
                        totalFailed.incrementAndGet();
                        logger.info("processed product id: {}, with status :: {} , processed till now :: {}/{}", id, false, totalProcessed.get(), totalCount);
                        continue;
                    }

                    Map<String, String> extratcIds = extractProductIdAndVariendId(response);

                    //saved ids in db
                    ProductIds pi = new ProductIds();
                    pi.setProductId(id);
                    pi.setShopifyProductId(extratcIds.get("product"));
                    productIdsRepository.save(pi);

                    getBaseVarientAndSetSkuAndPrice(extratcIds.get("product"), apiResponse);

                    List<JSONObject> metaFields = processMetafields(apiResponse);

                    processApiResponseAndUploadMetafields(extratcIds.get("product"), metaFields);

                    logger.info("Product created successfully for product id: " + id);
                    totalSuccess.incrementAndGet();

                    logger.info("processed product id: {}, with status :: {} , processed till now :: {}/{}", id, true, totalProcessed.get(), totalCount);
                } catch (Exception e) {
                    totalFailed.incrementAndGet();
                    logger.error("error in product create for id :: {} :: {}", id, e.getMessage(), e);
                }
            }

            logger.info("Import process complete with total processed :: {}/{} with success: {}, failed: {} and started at :: {} and ended at :: {}", totalProcessed.get(), totalCount, totalSuccess.get(), totalFailed.get(), startTime, ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z")));

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


        if (type.contains("text_field") || type.contains("number") || type.equals("boolean")) {
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
//        else if (type.equals("boolean")) {
//            value = value.toString();
//        }

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


    private JSONObject fetchProductDetailsFromApi(String productId) {
        String url = "https://erp.abelini.com/shopify/api/product/product_detail.php";
        Map<String, String> request = new HashMap<>();
        request.put("product_id", productId);

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        headers.setBearerAuth(jwtToken);

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

        Map<String, Object> seo = new HashMap<>();
        if (apiResponse.has("meta_title")) {
            seo.put("title", apiResponse.optString("meta_title"));
        } else if (apiResponse.has("name")) {
            seo.put("title", apiResponse.optString("name"));
        }

        if (apiResponse.has("meta_description")) {
            seo.put("description", apiResponse.optString("meta_description"));
        } else if (apiResponse.has("description")) {
            seo.put("description", apiResponse.optString("description"));
        }

        response.put("seo", seo);

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
        addMetafield(processedMetafields, rawMetafields, "meta_keyword", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "upc", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "product_type_id", "number_integer");
        addMetafield(processedMetafields, rawMetafields, "diamond_selection", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "view_360", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "having_modal_image_single", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "having_image_view_single", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "having_carat_single", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "image_counter", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "product_minimum_price", "json", "minimum_price_json");
        addMetafield(processedMetafields, rawMetafields, "product_id", "number_integer", "open_cart_product_id");
        addMetafield(processedMetafields, rawMetafields, "product_options", "json", "option_json");

        addMetafield(processedMetafields, rawMetafields, "having_stone_type", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "having_stone_shape", "multi_line_text_field");

        addMetafield(processedMetafields, rawMetafields, "category_feed_id", "number_integer", "opencart_category_id");
        addMetafield(processedMetafields, rawMetafields, "style_feed_id", "number_integer", "opencart_style_id");

        addProductReferenceListMetafield(processedMetafields, rawMetafields, "matching_products", "matching_product_open_cart");
        addProductReferenceListMetafield(processedMetafields, rawMetafields, "related_products", "related_product_open_cart");

        addMetafield(processedMetafields, rawMetafields, "tag_no", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "certificate_number", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "is_quickship", "boolean");

        addMetafield(processedMetafields, rawMetafields, "is_child", "boolean");

        addMetafield(processedMetafields, rawMetafields, "location", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "quantity_text", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "quantity", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "sort_order", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "viewed", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "sold", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "delivery_days", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "multistone", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "rrp", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "setting_code", "single_line_text_field");

        addMetafield(processedMetafields, rawMetafields, "filter", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "model", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "single_image", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "image", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "title_logic", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "default_stone", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "rtr_sample_text", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "rtr_hide_options", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "rtr", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "design", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "model_sizes", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "how_it_fits_type", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "how_it_fits", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "single", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "best_seller", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "single_image_counter", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "option_shape", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "option_stone", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "weight", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "option_metal_wt", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "date_added", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "product_markup", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "gemstone", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "bandwidth", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "stonetype_display", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "carat_slider", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "carat_range", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "discount", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "markup", "single_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "keyword", "single_line_text_field");

        // Special metafields requiring processing
//      Setting Type
        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "product_options", "list.metaobject_reference", "setting_type", this::getSettingTypeIds);
//      Colour
        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "product_options", "list.metaobject_reference", "colour", this::getColourIds);
//      Metal
        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "product_options", "list.metaobject_reference", "metal", this::getMetalIds);
//      Certificate
        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "product_options", "list.metaobject_reference", "certificate", this::getCertificateIds);
//      Clarity
        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "product_options", "list.metaobject_reference", "clarity", this::getClarityIds);
//        Shape
        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "product_options", "list.metaobject_reference", "shape", this::getShapeIds);
//        Ring Size
        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "product_options", "list.metaobject_reference", "ring_size", this::getRingSizeIds);
//        By Occasion
        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "product_options", "list.metaobject_reference", "by_occasion", this::getOccasionIds);
//        Personalised
        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "product_options", "list.metaobject_reference", "personalised", this::getPersonalisedIds);
//        By Recipient
        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "product_options", "list.metaobject_reference", "by_recipient", this::getRecipientIds);
//        Carat
        addProcessedMetafield(processedMetafields, rawMetafields, "product_options", "product_options", "list.metaobject_reference", "carat", this::getCaratIds);
//        Band Width
        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "product_options", "list.metaobject_reference", "band_width", this::getBandWidthIds);
//        Stone type
        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "product_options", "list.metaobject_reference", "stone_type", this::getStoneTypeIds);
//        Style Product
        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "product_options", "list.metaobject_reference", "style_product", this::getStyleProductIds);
//        Category
        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "product_options", "list.metaobject_reference", "category", this::getCategoryIds);

//        addProcessedMetafield(processedMetafields, rawMetafields, "product_filters", "list.metaobject_reference", "backing", this::getBackingIds);

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

            Object value = rawMetafields.get(key);

            if (key.equals("image") || key.equals("single_image")) {
                value = value.toString().replace("/", "_");
            }

            if (type.equals("boolean")) {
                String boolStr = value.toString().trim().toLowerCase();
                if (!boolStr.equals("true") && !boolStr.equals("false")) {
                    logger.warn("Invalid boolean value for key {}: {}", key, value);
                    return;
                }
                value = boolStr;
            }

            metafield.put("namespace", "custom");
            metafield.put("key", metafieldKey);
            metafield.put("type", type);
            metafield.put("value", value);
            metafields.add(metafield);
        }
    }

    private void addProcessedMetafield(List<JSONObject> metafields, JSONObject rawMetafields, String key, String option, String type, String metafieldKey, BiFunction<Object, Object, Object> processor) throws JsonProcessingException {
        logger.info("checking for processed meta fields :: {} :: {}", key, option);
        if (rawMetafields.has(key) && !rawMetafields.isNull(key) &&
                rawMetafields.has(option) && !rawMetafields.isNull(option)) {
            logger.info("checking for success processed meta fields :: {} :: {}", key, option);
            JSONObject metafield = new JSONObject();
            metafield.put("namespace", "custom");
            metafield.put("key", metafieldKey);
            metafield.put("type", type);
            metafield.put("value", processor.apply(rawMetafields.get(key), rawMetafields.get(option)));
            metafields.add(metafield);
        }
    }

    private List<String> getRecipientIds(Object o, Object productOptions) {
        Set<String> names = new HashSet<>();
        List<String> filterNames = extractNamesByFilterGroupId(o, "8");
        List<String> optionNames = extractOptionNames(productOptions, "by_recipient");
        names.addAll(filterNames);
        names.addAll(optionNames);
        logger.info("extract recipient :: {}", names);
        List<String> ids = new ArrayList<>();
        for (String name : names) {
            if (recipientMap.containsKey(name)) {
                ids.add(recipientMap.get(name));
            }
        }
        return ids;
    }

    private List<String> getSettingTypeIds(Object o, Object productOptions) {
        Set<String> names = new HashSet<>();
        List<String> filterNames = extractNamesByFilterGroupId(o, "6");
        List<String> optionNames = extractOptionNames(productOptions, "setting_type");
        names.addAll(filterNames);
        names.addAll(optionNames);
        logger.info("extract setting type :: {}", names);
        List<String> ids = new ArrayList<>();
        for (String name : names) {
            if (settingTypeMap.containsKey(name)) {
                ids.add(settingTypeMap.get(name));
            }
        }
        return ids;
    }

    private List<String> getShapeIds(Object o, Object productOptions) {
        try {
            Set<String> names = new HashSet<>();
            List<String> filterNames = extractNamesByFilterGroupId(o, "4");
            List<String> optionNames = extractOptionNames(productOptions, "shape");
            names.addAll(filterNames);
            names.addAll(optionNames);
            logger.info("extract shape names :: {}", names);
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

    private List<String> getStoneTypeIds(Object o, Object productOptions) {
        try {
            Set<String> names = new HashSet<>();

            List<String> filterNames = extractNamesByFilterGroupId(o, "3");
            List<String> optionNames = extractOptionNames(productOptions, "stone_type");
            names.addAll(filterNames);
            names.addAll(optionNames);
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

    private List<String> getMetalIds(Object o, Object productOptions) {
        try {
            Set<String> names = new HashSet<>();

            List<String> filterNames = extractNamesByFilterGroupId(o, "1");
            List<String> optionNames = extractOptionNames(productOptions, "metal");
            names.addAll(filterNames);
            names.addAll(optionNames);
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

    private List<String> getCategoryIds(Object o, Object productOptions) {
        try {
            Set<String> names = new HashSet<>();
            List<String> filterNames = extractNamesByFilterGroupId(o, "10");
            List<String> optionNames = extractOptionNames(productOptions, "category");
            names.addAll(filterNames);
            names.addAll(optionNames);
            logger.info("extract category :: {}", names);
            List<String> ids = new ArrayList<>();
            for (String name : names) {
                if (categoryMap.containsKey(name)) {
                    ids.add(categoryMap.get(name));
                }
            }
            return ids;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private List<String> getStyleProductIds(Object o, Object productOptions) {
        try {
            Set<String> names = new HashSet<>();
            List<String> filterNames = extractNamesByFilterGroupId(o, "5");
            List<String> optionNames = extractOptionNames(productOptions, "style");
            names.addAll(filterNames);
            names.addAll(optionNames);
            logger.info("extract style product :: {}", names);
            List<String> ids = new ArrayList<>();
            for (String name : names) {
                if (styleProductMap.containsKey(name)) {
                    ids.add(styleProductMap.get(name));
                }
            }
            return ids;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private List<String> getBandWidthIds(Object o, Object productOptions) {
        try {
            Set<String> names = new HashSet<>();
            List<String> filterNames = extractNamesByFilterGroupId(o, "11");
            List<String> optionNames = extractOptionNames(productOptions, "band_width");
            names.addAll(filterNames);
            names.addAll(optionNames);
            logger.info("extract band width :: {}", names);
            List<String> ids = new ArrayList<>();
            for (String name : names) {
                if (bandWidthMap.containsKey(name)) {
                    ids.add(bandWidthMap.get(name));
                }
            }
            return ids;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private List<String> getPersonalisedIds(Object o, Object productOptions) {
        try {
            Set<String> names = new HashSet<>();
            List<String> filterNames = extractNamesByFilterGroupId(o, "9");
            List<String> optionNames = extractOptionNames(productOptions, "personalised");
            names.addAll(filterNames);
            names.addAll(optionNames);
            logger.info("extract personalized :: {}", names);
            List<String> ids = new ArrayList<>();
            for (String name : names) {
                if (personilizationMap.containsKey(name)) {
                    ids.add(personilizationMap.get(name));
                }
            }
            return ids;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private List<String> getCaratIds(Object o, Object productOptions) {
        try {
            Set<String> names = new HashSet<>();
            List<String> filterNames = extractNamesByFilterGroupId(o, "17");
            List<String> optionNames = extractOptionNames(productOptions, "carat");
            names.addAll(filterNames);
            names.addAll(optionNames);
            logger.info("extract carat :: {}", names);
            List<String> ids = new ArrayList<>();
            for (String name : names) {
                if (caratMap.containsKey(name)) {
                    ids.add(caratMap.get(name));
                }
            }
            return ids;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private List<String> getOccasionIds(Object o, Object productOptions) {
        try {
            Set<String> names = new HashSet<>();
            List<String> filterNames = extractNamesByFilterGroupId(o, "7");
            List<String> optionNames = extractOptionNames(productOptions, "by_occasion");
            names.addAll(filterNames);
            names.addAll(optionNames);
            logger.info("extract occasion :: {}", names);
            List<String> ids = new ArrayList<>();
            for (String name : names) {
                if (occasionMap.containsKey(name)) {
                    ids.add(occasionMap.get(name));
                }
            }
            return ids;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private List<String> getRingSizeIds(Object o, Object productOptions) {
        try {
            Set<String> names = new HashSet<>();
            List<String> filterNames = extractNamesByFilterGroupId(o, "16");
            List<String> optionNames = extractOptionNames(productOptions, "ring_size");
            names.addAll(filterNames);
            names.addAll(optionNames);
            logger.info("extract ring size :: {}", names);
            List<String> ids = new ArrayList<>();
            for (String name : names) {
                if (ringSizeMap.containsKey(name)) {
                    ids.add(ringSizeMap.get(name));
                }
            }
            return ids;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private List<String> getClarityIds(Object o, Object productOptions) {
        try {
            Set<String> names = new HashSet<>();
            List<String> filterNames = extractNamesByFilterGroupId(o, "14");
            List<String> optionNames = extractOptionNames(productOptions, "clarity");
            names.addAll(filterNames);
            names.addAll(optionNames);
            logger.info("extract clarity :: {}", names);
            List<String> ids = new ArrayList<>();
            for (String name : names) {
                if (clarityMap.containsKey(name)) {
                    ids.add(clarityMap.get(name));
                }
            }
            return ids;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private List<String> getCertificateIds(Object o, Object productOptions) {
        try {
            Set<String> names = new HashSet<>();
            List<String> filterNames = extractNamesByFilterGroupId(o, "15");
            List<String> optionNames = extractOptionNames(productOptions, "certificate");
            names.addAll(filterNames);
            names.addAll(optionNames);
            logger.info("extract certificate :: {}", names);
            List<String> ids = new ArrayList<>();
            for (String name : names) {
                if (certificateMap.containsKey(name)) {
                    ids.add(certificateMap.get(name));
                }
            }
            return ids;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private List<String> getColourIds(Object o, Object productOptions) {
        try {
            Set<String> names = new HashSet<>();
            List<String> filterNames = extractNamesByFilterGroupId(o, "2");
            List<String> optionNames = extractOptionNames(productOptions, "color");
            names.addAll(filterNames);
            names.addAll(optionNames);
            logger.info("extract colour :: {}", names);
            List<String> ids = new ArrayList<>();
            for (String name : names) {
                if (colourMap.containsKey(name)) {
                    ids.add(colourMap.get(name));
                }
            }
            return ids;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

//    private Object getBackingIds(Object backing) {
//        try {
//            logger.info("inside get backing ids");
//            List<String> names = extractOptionNames(backing, "backing");
//            logger.info("extract backing :: {}", names);
//            List<String> ids = new ArrayList<>();
//            for (String name : names) {
//                if (backingMap.containsKey(name)) {
//                    ids.add(backingMap.get(name));
//                }
//            }
//            return ids;
//        } catch (Exception e) {
//            return new ArrayList<>();
//        }
//    }

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

            if (rootNode.isArray()) {
                // Process JSON if it's an array
                for (JsonNode filter : rootNode) {
                    processFilterNode(filter, targetFilterGroupId, names);
                }
            } else if (rootNode.isObject()) {
                // Process JSON if it's an object
                for (JsonNode filter : rootNode) {
                    processFilterNode(filter, targetFilterGroupId, names);
                }
            } else {
                logger.warn("Unexpected JSON structure. Expected an array or object.");
            }

        } catch (Exception e) {
            logger.error("Error parsing JSON: {}", e.getMessage(), e);
        }

        return names;
    }

    private void processFilterNode(JsonNode filter, String targetFilterGroupId, List<String> names) {
        if (filter.has("filter_group_id") && filter.has("name")) {
            String filterGroupId = filter.get("filter_group_id").asText();

            if (targetFilterGroupId.equals(filterGroupId)) {
                String name = filter.get("name").asText();
                logger.info("Found name: {}", name);
                names.add(name);
            }
        }
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

    private List<String> readCSVFromPath(String filePath) throws IOException, CsvException {
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            List<String[]> records = reader.readAll();
            return records.stream().skip(1) // Skip header row
                    .map(row -> row[0])
                    .collect(Collectors.toList());
        }
    }

    public ResponseEntity<?> getMissingProducts() throws IOException, CsvException {
        String filePath = "src/main/resources/s3file/active-products.csv";
        List<String> ids = readCSVFromPath(filePath);

        // Fetch all product IDs from the database
        List<ProductIds> products = productIdsRepository.findAll();
        Set<String> existingProductIds = products.stream()
                .map(ProductIds::getProductId)
                .collect(Collectors.toSet());

        // Find missing IDs (present in CSV but not in DB)
        List<String> missingIds = ids.stream()
                .filter(id -> !existingProductIds.contains(id))
                .collect(Collectors.toList());

        // Return missing IDs
        return ResponseEntity.ok(missingIds);
    }

    @Async
    public void addProductsToCollection(String collectionId, List<String> productIdToShopifyId) {

        logger.info("total product ids count ::{}", productIdToShopifyId.size());

        int batchSize = 240;
        int totalProcessed = 0;
        int batchNumber = 0;

        for (int i = 0; i < productIdToShopifyId.size(); i += batchSize) {
            regulateApiRate();
            remainingPoints.addAndGet(-API_COST_PER_CALL);
            batchNumber++;
            List<String> batch = productIdToShopifyId.subList(i, Math.min(i + batchSize, productIdToShopifyId.size()));
            logger.info("Start Processing batch {} ({} - {}), Batch size: {}",
                    batchNumber, i + 1, i + batch.size(), batch.size());

            try {
                processBatch(collectionId, batch);
                totalProcessed += batch.size();
                logger.info("Completed batch {}. Total processed so far: {}", batchNumber, totalProcessed);
            } catch (Exception e) {
                logger.info("error while processing batch {}", batchNumber);
            }
        }
        logger.info("All batches processed. Total products added: {}", totalProcessed);
        logger.info("Import product in collection process complete :: {}", ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z")));
    }

    private void processBatch(String collectionId, List<String> batch) {
        StringBuilder mutation = new StringBuilder();
        mutation.append("mutation { collectionAddProducts(id: \"")
                .append(collectionId)
                .append("\", productIds: [");

        for (String productGid : batch) {
            mutation.append("\"").append(productGid).append("\", ");
        }

        if (!batch.isEmpty()) {
            mutation.setLength(mutation.length() - 2);
        }

        mutation.append("]) { collection { id } userErrors { field message } } }");

        logger.info("Generated Mutation: {}", mutation);

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("query", mutation.toString());

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        headers.set("X-Shopify-Access-Token", accessToken);

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestBody, headers);
        ResponseEntity<String> response = restTemplate.postForEntity(shopifyStore + "/admin/api/2025-01/graphql.json", request, String.class);

        logger.info("Shopify Response: {}", response.getBody());
    }

    @Async
    public void productVarientMigration(boolean isTest) {
        try {
            String startTime = ZonedDateTime.now(ZoneId.of("Asia/Kolkata"))
                    .format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z"));
            logger.info("Starting product import at: {}", startTime);

            String json;

            if (!isTest) {
                logger.info("fetching live all_stock_product");
                String firstApiUrl = "https://www.abelini.com/shopify/api/all_stock_product.php";

                ResponseEntity<String> firstApiResponse = restTemplate.exchange(
                        firstApiUrl,
                        HttpMethod.POST,
                        null,
                        String.class
                );

                if (firstApiResponse.getStatusCode() != HttpStatus.OK) {
                    logger.error("Error fetching all stock products from Abelini API. Response: {}", firstApiResponse.getBody());
                    return;
                }

                json = firstApiResponse.getBody();
            } else {
                logger.info("fetching test all_stock_product");
                json = """
                        [
                          {
                              "product_id": "1228",
                              "sort_order": "0",
                              "parent_id": "1552646",
                              "stock_id": "40749",
                              "tag_no": "VR-61534"
                          },
                          {
                              "product_id": "1228",
                              "sort_order": "0",
                              "parent_id": "1552840",
                              "stock_id": "50603",
                              "tag_no": "VR-71000"
                          },
                          {
                              "product_id": "1228",
                              "sort_order": "0",
                              "parent_id": "1553007",
                              "stock_id": "57173",
                              "tag_no": "VR-77701"
                          },
                          {
                              "product_id": "1228",
                              "sort_order": "0",
                              "parent_id": "1553172",
                              "stock_id": "64819",
                              "tag_no": "VR-85466"
                          },
                          {
                              "product_id": "1228",
                              "sort_order": "0",
                              "parent_id": "1553410",
                              "stock_id": "70439",
                              "tag_no": "VR-90958"
                          },
                          {
                              "product_id": "1228",
                              "sort_order": "0",
                              "parent_id": "1553411",
                              "stock_id": "70496",
                              "tag_no": "VR-90893"
                          },
                          {
                              "product_id": "1228",
                              "sort_order": "0",
                              "parent_id": "1553422",
                              "stock_id": "70762",
                              "tag_no": "VR-91051"
                          },
                          {
                              "product_id": "1228",
                              "sort_order": "0",
                              "parent_id": "1553568",
                              "stock_id": "71563",
                              "tag_no": "VR-91831"
                          }
                        ]
                        """;
            }

            if (StringUtils.isBlank(json)) {
                logger.info("json response is empty");
                return;
            }

            List<Map<String, Object>> productList = objectMapper.readValue(json, new TypeReference<>() {
            });

            long totalProductsVarient = productList.size();
            Set<String> uniqueProductIds = productList.stream()
                    .map(m -> m.get("product_id").toString())
                    .collect(Collectors.toSet());

            AtomicInteger totalProcessed = new AtomicInteger(0);
            AtomicInteger totalVariants = new AtomicInteger(0);
            AtomicInteger totalProductSuccess = new AtomicInteger(0);
            AtomicInteger totalProductFailed = new AtomicInteger(0);
            AtomicInteger totalVariantSuccess = new AtomicInteger(0);
            AtomicInteger totalVariantFailed = new AtomicInteger(0);

            logger.info("Total products varient: {}", totalProductsVarient);
            logger.info("Unique product count: {}", uniqueProductIds.size());

            for (String id : uniqueProductIds) {
                try {
                    totalProcessed.incrementAndGet();
                    logger.info("Processing product ID: {}", id);
                    JSONArray apiResponseArray = fetchProductVarientDetailsFromApi(id);

                    if (apiResponseArray == null || apiResponseArray.isEmpty()) {
                        logger.error("No variants found for product ID: {}", id);
                        totalProductFailed.incrementAndGet();
                        continue;
                    }

                    int variantCount = apiResponseArray.length();
                    totalVariants.addAndGet(variantCount);
                    AtomicInteger processedVariants = new AtomicInteger(0);

                    for (int i = 0; i < apiResponseArray.length(); i++) {
                        JSONObject apiResponse = apiResponseArray.getJSONObject(i);
                        String tagNo = apiResponse.optString("tag_no", "N/A");
                        logger.info("starting product id :: {}, varient tag no :: {}", id, tagNo);
                        processedVariants.incrementAndGet();

                        Map<String, Object> data = processResponse(apiResponse);
                        regulateApiRate();
                        remainingPoints.addAndGet(-API_COST_PER_CALL);

                        Map<String, Object> product = new HashMap<>();
                        product.put("product", data);
                        String response = sendGraphQLRequest(GRAPHQL_QUERY_PRODUCTS_CREATE, objectMapper.writeValueAsString(product), false);

                        if (response == null) {
                            logger.error("Shopify response null for product ID: {}, tag_no: {}", id, tagNo);
                            totalVariantFailed.incrementAndGet();
                            continue;
                        }

                        Map<String, String> extractIds = extractProductIdAndVariendId(response);

                        ProductVarientIds pi = new ProductVarientIds();
                        pi.setProductId(id);
                        pi.setShopifyProductId(extractIds.get("product"));
                        pi.setTagNo(tagNo);
                        productVarientIdsRepository.save(pi);

                        getBaseVarientAndSetSkuAndPrice2(extractIds.get("product"), apiResponse);
                        List<JSONObject> metaFields = processMetafields(apiResponse);
                        processApiResponseAndUploadMetafields(extractIds.get("product"), metaFields);

                        logger.info("Successfully created variant for product ID: {}, tag_no: {}", id, tagNo);
                        totalVariantSuccess.incrementAndGet();
                    }

                    if (processedVariants.get() > 0) {
                        totalProductSuccess.incrementAndGet();
                    }

                    logger.info("Completed product ID: {} with {} variants processed", id, variantCount);
                } catch (Exception e) {
                    totalProductFailed.incrementAndGet();
                    logger.error("Error processing product ID: {} :: {}", id, e.getMessage(), e);
                }
            }

            String endTime = ZonedDateTime.now(ZoneId.of("Asia/Kolkata"))
                    .format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z"));

            logger.info("Import process complete! Products processed: {}/{}, Success: {}, Failed: {} | Variants processed: {}/{}, Success: {}, Failed: {} | Started at: {}, Ended at: {}",
                    totalProcessed.get(), uniqueProductIds.size(), totalProductSuccess.get(), totalProductFailed.get(),
                    totalVariants.get(), totalProductsVarient, totalVariantSuccess.get(), totalVariantFailed.get(), startTime, endTime);

            logger.info("start collection import process");
            Map<String, ProductEntry> productEntryMap = new LinkedHashMap<>();
            for (Map<String, Object> map : productList) {
                String productId = String.valueOf(map.get("product_id"));
                String tagNo = String.valueOf(map.get("tag_no"));
                int sortOrder = Integer.parseInt(String.valueOf(map.get("sort_order")));

                String key = productId + "::" + tagNo;
                productEntryMap.putIfAbsent(key, new ProductEntry(productId, tagNo, sortOrder));
            }

            logger.info("Unique (product_id + tag_no) entries to process: {}", productEntryMap.size());

            // Fetch all variant records
            List<ProductVarientIds> allVariants = productVarientIdsRepository.findAll();

            // Build a lookup map: (productId + "::" + tagNo) -> shopifyProductId
            Map<String, String> variantLookup = allVariants.stream()
                    .collect(Collectors.toMap(
                            v -> v.getProductId() + "::" + v.getTagNo(),
                            ProductVarientIds::getShopifyProductId,
                            (a, b) -> a
                    ));

            // Sort and collect matching Shopify product IDs
            List<String> shopifyIds = productEntryMap.values().stream()
                    .sorted(Comparator.comparingInt(ProductEntry::getSortOrder))
                    .map(entry -> variantLookup.get(entry.getProductId() + "::" + entry.getTagNo()))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            logger.info("Final Shopify product IDs to migrate: {}", shopifyIds.size());

            // Call migration
            addProductsToCollection("gid://shopify/Collection/675274621268", shopifyIds);

            logger.info("Import process complete! Products processed: {}/{}, Success: {}, Failed: {} | Variants processed: {}/{}, Success: {}, Failed: {} | Started at: {}, Ended at: {}",
                    totalProcessed.get(), uniqueProductIds.size(), totalProductSuccess.get(), totalProductFailed.get(),
                    totalVariants.get(), totalProductsVarient, totalVariantSuccess.get(), totalVariantFailed.get(), startTime, endTime);
        } catch (Exception e) {
            logger.error("Unexpected error in product migration: {}", e.getMessage(), e);
        }
    }

    private JSONArray fetchProductVarientDetailsFromApi(String id) {
        String url = "https://www.abelini.com/shopify/api/stock_product.php";
        Map<String, String> request = new HashMap<>();
        request.put("product_id", id);

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        HttpEntity<Map<String, String>> entity = new HttpEntity<>(request, headers);

        String response = restTemplate.postForObject(url, entity, String.class);
//        logger.info("api response :: {}", response);
        try {
            JSONArray jsonArray = new JSONArray(response);
            if (!jsonArray.isEmpty()) {
                return jsonArray;
            } else {
                logger.error("API returned an empty list for product ID: {}", id);
                return null;
            }
        } catch (Exception e) {
            System.out.println("Failed to parse product details JSON: " + e.getMessage());
            return null;
        }
    }

    private void getBaseVarientAndSetSkuAndPrice2(String productId, JSONObject apiResponse) {
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
                variables.put("sku", apiResponse.optString("sku") + "_" + apiResponse.optString("tag_no"));
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

    @Async
    public void importedProduct2FieldReUpload() {
        try {
            String startTime = ZonedDateTime.now(ZoneId.of("Asia/Kolkata"))
                    .format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z"));
            logger.info("Starting product import at: {}", startTime);

            AtomicInteger totalProcessed = new AtomicInteger(0);
            AtomicInteger totalSuccess = new AtomicInteger(0);
            AtomicInteger totalFailed = new AtomicInteger(0);

            List<ProductIds> productIds = productIdsRepository.findAll();

            if (productIds.isEmpty()) {
                logger.error("no id found");
                return;
            }
            long totalCount = productIds.size();

            for (ProductIds product : productIds) {
                totalProcessed.incrementAndGet();
                String id = product.getProductId();
                try {
                    logger.info("Processing product id: " + id);
                    JSONObject apiResponse = fetchProductDetailsFromApi(id);

                    if (apiResponse == null || apiResponse.isEmpty()) {
                        logger.error("null or empty error while creating product id: " + id);
                        totalFailed.incrementAndGet();
                        logger.info("processed product id: {}, with status :: {} , processed till now :: {}/{}", id, false, totalProcessed.get(), totalCount);
                        continue;
                    }

                    List<JSONObject> metaFields = process2Metafields(apiResponse);

                    processApiResponseAndUploadMetafields(product.getShopifyProductId(), metaFields);

                    logger.info("Product created successfully for product id: " + id);
                    totalSuccess.incrementAndGet();

                    logger.info("processed product id: {}, with status :: {} , processed till now :: {}/{}", id, true, totalProcessed.get(), totalCount);
                } catch (Exception e) {
                    logger.error("Error processing product id :: {}", id);
                }
            }

            String endTime = ZonedDateTime.now(ZoneId.of("Asia/Kolkata"))
                    .format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z"));
            logger.info("Import 2 field process completed with total processed :: {}/{} with success: {}, failed: {} and started at :: {} and ended at :: {}",
                    totalProcessed.get(), totalCount, totalSuccess.get(), totalFailed.get(),
                    startTime, endTime);
        } catch (Exception e) {
            logger.error("Error processing the importedProduct2FieldReUpload : {}", e.getMessage(), e);
        }
    }

    private List<JSONObject> process2Metafields(JSONObject rawMetafields) {
        List<JSONObject> processedMetafields = new ArrayList<>();

        addMetafield(processedMetafields, rawMetafields, "having_stone_type", "multi_line_text_field");
        addMetafield(processedMetafields, rawMetafields, "having_stone_shape", "multi_line_text_field");

        return processedMetafields;
    }

    @Async
    public void importedProduct2FieldReUploadSecond() {
        try {
            String startTime = ZonedDateTime.now(ZoneId.of("Asia/Kolkata"))
                    .format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z"));
            logger.info("Starting product import at: {}", startTime);

            AtomicInteger totalProcessed = new AtomicInteger(0);
            AtomicInteger totalSuccess = new AtomicInteger(0);
            AtomicInteger totalFailed = new AtomicInteger(0);

            List<ProductIds> productIds = productIdsRepository.findAll();
//            List<ProductIds> productIds = productIdsRepository.findByProductId("1228");
            if (productIds.isEmpty()) {
                logger.error("no id found");
                return;
            }
            long totalCount = productIds.size();

            for (ProductIds product : productIds) {
                totalProcessed.incrementAndGet();
                String id = product.getProductId();
                try {
                    logger.info("Processing product id: " + id);
                    JSONObject apiResponse = fetchProductDetailsFromApi(id);

                    if (apiResponse == null || apiResponse.isEmpty()) {
                        logger.error("null or empty error while creating product id: " + id);
                        totalFailed.incrementAndGet();
                        logger.info("processed product id: {}, with status :: {} , processed till now :: {}/{}", id, false, totalProcessed.get(), totalCount);
                        continue;
                    }

                    List<JSONObject> metaFields = process2MetafieldsSecond(apiResponse);

                    processApiResponseAndUploadMetafields(product.getShopifyProductId(), metaFields);

                    logger.info("Product created successfully for product id: " + id);
                    totalSuccess.incrementAndGet();

                    logger.info("processed product id: {}, with status :: {} , processed till now :: {}/{}", id, true, totalProcessed.get(), totalCount);
                } catch (Exception e) {
                    logger.error("Error processing product id :: {}", id);
                }
            }

            String endTime = ZonedDateTime.now(ZoneId.of("Asia/Kolkata"))
                    .format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z"));
            logger.info("Import 2 field process completed with total processed :: {}/{} with success: {}, failed: {} and started at :: {} and ended at :: {}",
                    totalProcessed.get(), totalCount, totalSuccess.get(), totalFailed.get(),
                    startTime, endTime);
        } catch (Exception e) {
            logger.error("Error processing the importedProduct2FieldReUpload : {}", e.getMessage(), e);
        }
    }

    private List<JSONObject> process2MetafieldsSecond(JSONObject rawMetafields) {
        List<JSONObject> processedMetafields = new ArrayList<>();

        addMetafield(processedMetafields, rawMetafields, "category_feed_id", "number_integer", "opencart_category_id");
        addMetafield(processedMetafields, rawMetafields, "style_feed_id", "number_integer", "opencart_style_id");

        addProductReferenceListMetafield(processedMetafields, rawMetafields, "matching_products", "matching_product_open_cart");
        addProductReferenceListMetafield(processedMetafields, rawMetafields, "related_products", "related_product_open_cart");
        return processedMetafields;
    }

    private void addProductReferenceListMetafield(List<JSONObject> metafields, JSONObject raw, String
            key, String metafieldName) {
        if (raw.has(key)) {
            JSONArray idsArray = raw.optJSONArray(key);
            if (idsArray == null && raw.get(key) instanceof String) {
                idsArray = new JSONArray().put(raw.getString(key));
            }

            if (idsArray != null && !idsArray.isEmpty()) {
                logger.info(metafieldName + " found size: " + idsArray.length());
                List<String> idList = new ArrayList<>();
                for (int i = 0; i < idsArray.length(); i++) {
                    idList.add(idsArray.getString(i));
                }

                // Fetch GIDs from DB
                List<ProductIds> products = productIdsRepository.findAllById(idList);
                logger.info("products my db found : {}/{}", products.size(), idsArray.length());
                JSONArray gidList = new JSONArray();
                for (ProductIds product : products) {
                    gidList.put(product.getShopifyProductId()); // Assuming this returns gid://shopify/Product/...
                }

                // Build metafield JSON
                JSONObject metafield = new JSONObject();
                metafield.put("namespace", "custom");
                metafield.put("key", metafieldName);
                metafield.put("type", "list.product_reference");
                metafield.put("value", gidList.toString());

                metafields.add(metafield);
            }
        }
    }

    private static final String CSV_FILE = "src/main/resources/log/variant_processing_log_28-05-25-final.csv";
    private static final AtomicBoolean headerWritten = new AtomicBoolean(false);
    private static final String BASE_URL = "https://erp.abelini.com/shopify/api/product/";
    private final AtomicInteger totalProducts = new AtomicInteger(0);
    private final AtomicInteger productSuccess = new AtomicInteger(0);
    private final AtomicInteger productFailed = new AtomicInteger(0);
    private final AtomicInteger totalVariants = new AtomicInteger(0);
    private final AtomicInteger variantSuccess = new AtomicInteger(0);
    private final AtomicInteger variantFailed = new AtomicInteger(0);

    private void createFileIfMissing() {
        try {
            Path path = Paths.get(CSV_FILE);
            if (!Files.exists(path)) {
                Files.createDirectories(path.getParent());
                Files.writeString(path, "product_id,variant_id,page,status,shopify_id\n", StandardOpenOption.CREATE);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error creating log CSV file", e);
        }
    }

    public void logVariant(String productId, String variantId, int page, boolean success, String shopifyId) {
        totalVariants.incrementAndGet();
        if (success) variantSuccess.incrementAndGet();
        else variantFailed.incrementAndGet();

        writeToCsv(productId, variantId, page, success ? "SUCCESS" : "FAILED", shopifyId.isBlank() ? "NA" : shopifyId);
    }

    public void logProduct(String productId, boolean success) {
        totalProducts.incrementAndGet();
        if (success) productSuccess.incrementAndGet();
        else productFailed.incrementAndGet();
    }

    private void writeToCsv(String productId, String variantId, int page, String status, String shopifyId) {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(CSV_FILE), StandardOpenOption.APPEND)) {
            writer.write(String.format("%s,%s,%d,%s,%s%n", productId, variantId, page, status, shopifyId));
        } catch (IOException e) {
            System.err.println("Failed to write log entry: " + e.getMessage());
        }
    }

    public void printSummary() {
        System.out.println("\n======= IMPORT SUMMARY =======");
        System.out.printf("Total Products Processed: %d%n", totalProducts.get());
        System.out.printf("Product Success: %d%n", productSuccess.get());
        System.out.printf("Product Failed: %d%n", productFailed.get());

        System.out.printf("Total Variants Processed: %d%n", totalVariants.get());
        System.out.printf("Variant Success: %d%n", variantSuccess.get());
        System.out.printf("Variant Failed: %d%n", variantFailed.get());
        System.out.println("CSV log saved to: " + CSV_FILE);
        System.out.println("=================================\n");
    }

    @Async
    public void imported2LakhProduct(boolean isTest) {
        try {
            String startTime = ZonedDateTime.now(ZoneId.of("Asia/Kolkata"))
                    .format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z"));
            logger.info("Starting import 2 Lakh Product at: {}", startTime);

            if (isTest) {
                List<Map<String, Object>> productList = new ArrayList<>();

                Map<String, Object> productl = new HashMap<>();
                productl.put("product_id", "459");
                productl.put("total_page", 1);
                productList.add(productl);
                for (Map<String, Object> product : productList) {
                    String productId = String.valueOf(product.get("product_id"));
                    int totalPages = Integer.parseInt(String.valueOf(product.get("total_page")));
                    for (int page = 1; page <= totalPages; page++) {
                        callProductDetails(productId, page);
                    }
                }
            } else {
                // 1. Get all products
                String allProductsUrl = BASE_URL + "all_products.php";
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                headers.setBearerAuth(jwtToken);
                HttpEntity<?> request = new HttpEntity<>(headers);
                ResponseEntity<String> response = restTemplate.postForEntity(allProductsUrl, request, String.class);

                if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                    List<Map<String, Object>> productList = objectMapper.readValue(
                            response.getBody(),
                            new TypeReference<>() {
                            }
                    );

                    // 2. For each product, loop over pages and call detail API
                    for (Map<String, Object> product : productList) {
                        String productId = String.valueOf(product.get("product_id"));
                        int totalPages = Integer.parseInt(String.valueOf(product.get("total_page")));
                        try {
                            if (!productId.isBlank() && totalPages > 0) {
                                if ("459".equals(productId)) {
                                    continue;
                                }
                                for (int page = 1; page <= totalPages; page++) {
                                    callProductDetails(productId, page);
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                } else {
                    System.err.println("Failed to fetch products: " + response.getStatusCode());
                }
            }

            printSummary();

            String endTime = ZonedDateTime.now(ZoneId.of("Asia/Kolkata"))
                    .format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z"));
            logger.info("2 lakh product import process ended..... started at : {} and ended at : {}", startTime, endTime);
        } catch (Exception e) {
            logger.error("Error while importing 2 lakh product", e);
            e.printStackTrace();
        }
    }


    private void callProductDetails(String productId, int page) {
        String detailsUrl = BASE_URL + "all_product_pnc.php";

        Map<String, String> payload = new HashMap<>();
        payload.put("product_id", productId);
        payload.put("page", String.valueOf(page));
        payload.put("limit", "50");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(jwtToken);
        HttpEntity<Map<String, String>> request = new HttpEntity<>(payload, headers);

        try {
            ResponseEntity<String> response = restTemplate.postForEntity(detailsUrl, request, String.class);
            System.out.printf("Fetched page %d for product %s%n", page, productId);
            if (response.getStatusCode() == HttpStatus.OK) {
                String json = response.getBody();
                List<Map<String, Object>> productList = objectMapper.readValue(json, new TypeReference<>() {
                });

                if (!productList.isEmpty()) {
                    for (Map<String, Object> product : productList) {
                        String variantId = String.valueOf(product.get("code"));

                        String shopifyId = importProductShopify(product, variantId, productId);
                        logVariant(productId, variantId, page, shopifyId != null, shopifyId);
                    }

                    logProduct(productId, true);
                } else {
                    logProduct(productId, false);
                }
            } else {
                logProduct(productId, false);
                System.err.printf("Failed to fetch page %d for product %s: %s%n", page, productId, response.getStatusCode());
            }
        } catch (Exception e) {
            logProduct(productId, false);
            System.err.printf("Error while calling product detail for %s page %d: %s%n", productId, page, e.getMessage());
        }
    }

    private String importProductShopify(Map<String, Object> product1, String variantId, String productId) {
        try {
            JSONObject apiResponse = new JSONObject(product1);
            Map<String, Object> data = processResponse(apiResponse);
            regulateApiRate();
            remainingPoints.addAndGet(-API_COST_PER_CALL);

            Map<String, Object> product = new HashMap<>();
            product.put("product", data);
            String response = sendGraphQLRequest(GRAPHQL_QUERY_PRODUCTS_CREATE, objectMapper.writeValueAsString(product), false);

            if (response == null) {
                logger.error("Shopify response null for product ID: {}, variant id: {}", productId, variantId);
                return null;
            }

            Map<String, String> extractIds = extractProductIdAndVariendId(response);

            Product2Lakh pi = new Product2Lakh();
            pi.setProductId(productId);
            pi.setShopifyProductId(extractIds.get("product"));
            pi.setVariantCode(variantId);
            product2lakhRepository.save(pi);

            getBaseVarientAndSetSkuAndPrice3(extractIds.get("product"), apiResponse);
            List<JSONObject> metaFields = processMetafields(apiResponse);
            processApiResponseAndUploadMetafields(extractIds.get("product"), metaFields);

            logger.info("Successfully created variant for product ID: {}, variant id: {}", productId, variantId);
            return extractIds.get("product");
        } catch (Exception e) {
            logger.error("Error while creating product id: {}, variant id: {}", productId, variantId);
            return null;
        }
    }

    private void getBaseVarientAndSetSkuAndPrice3(String productId, JSONObject apiResponse) {
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

            if (apiResponse.has("code")) {
                variables.put("sku", apiResponse.optString("code"));
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

    @Async
    public void minPriceUpdateBaseProduct() {
        try {
            String startTime = ZonedDateTime.now(ZoneId.of("Asia/Kolkata"))
                    .format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z"));
            logger.info("Starting minPriceUpdateBaseProduct Product at: {}", startTime);

            List<ProductIds> productIds = productIdsRepository.findAll();

            AtomicInteger totalProcessed = new AtomicInteger(0);
            AtomicInteger totalSuccess = new AtomicInteger(0);
            AtomicInteger totalFailed = new AtomicInteger(0);
            List<String> failedIds = new ArrayList<>();

            for (ProductIds product : productIds) {
                try {
                    totalProcessed.incrementAndGet();
                    JSONObject apiResponse = fetchProductDetailsFromApi(product.getProductId());
                    if (apiResponse == null || apiResponse.isEmpty()) {
                        logger.error("null or empty error while creating product id: " + product.getProductId());
                        totalFailed.incrementAndGet();
                        failedIds.add(product.getProductId());
                        continue;
                    }

                    Map<String, Object> data = processResponse(apiResponse);
                    if (data == null) {
                        logger.error("data null error while creating product id: " + product.getProductId());
                        totalFailed.incrementAndGet();
                        failedIds.add(product.getProductId());
                        continue;
                    } else {
                        data.put("id", product.getShopifyProductId());
                    }

                    regulateApiRate();
                    remainingPoints.addAndGet(-API_COST_PER_CALL);

                    Map<String, Object> input = new HashMap<>();
                    input.put("input", data);
                    String response = sendGraphQLRequest(GRAPHQL_QUERY_PRODUCTS_UPDATE, objectMapper.writeValueAsString(input), false);

                    if (response == null) {
                        logger.error("shopify response null error while creating product id: " + product.getProductId());
                        totalFailed.incrementAndGet();
                        failedIds.add(product.getProductId());
                        continue;
                    }

                    getBaseVarientAndSetSkuAndPrice(product.getShopifyProductId(), apiResponse);

                    List<JSONObject> metaFields = processMetafields(apiResponse);

                    processApiResponseAndUploadMetafields(product.getShopifyProductId(), metaFields);

                    logger.info("Product updated successfully for product id: " + product.getProductId());
                    totalSuccess.incrementAndGet();
                } catch (Exception e) {
                    failedIds.add(product.getProductId());
                    totalFailed.incrementAndGet();
                    logger.error("Exception while minPriceUpdateBaseProduct for product id:{} ::: ", product.getProductId(), e);
                }
            }

            String endTime = ZonedDateTime.now(ZoneId.of("Asia/Kolkata"))
                    .format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z"));
            logger.info("Completed minPriceUpdateBaseProduct Product started at :{} and ended at :{} :::  total count :{}, processed count :{}, success count :{}, failed count :{}, failed ids:{}", startTime, endTime, productIds.size(), totalProcessed.get(), totalSuccess.get(), totalFailed.get(), failedIds);
        } catch (Exception e) {
            logger.error("Exception while minPriceUpdateBaseProduct ::: ", e);
        }
    }
}