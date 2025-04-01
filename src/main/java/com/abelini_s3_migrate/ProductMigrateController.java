package com.abelini_s3_migrate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/product/migrate")
public class ProductMigrateController {
    private static final Logger logger = LoggerFactory.getLogger(ProductMigrateController.class);
    @Autowired
    private ProductMigrationService migrationService;
    @Autowired
    private ProductIdsRepository productIdsRepository;

    @PostMapping("/upload")
    public ResponseEntity<String> uploadFile(@RequestParam(value = "file", required = false) MultipartFile file,
                                             @RequestParam(defaultValue = "0", required = false) String id) {
        try {
            migrationService.processProducts(file, id);
            return ResponseEntity.ok("Migration started successfully");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage());
        }
    }

    @GetMapping("/missing")
    public ResponseEntity<?> getMissingProducts() {
        try {
            return migrationService.getMissingProducts();
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage());
        }
    }

    @PostMapping("/import-product-collection")
    public String importProductsToCollection(@RequestParam String collectionId, @RequestBody List<ProductSortOrder> products) {
        String startTime = ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z"));
        logger.info("Starting product import in collection... at:: {}", startTime);

        logger.info("payload size :: {}", products.size());

        List<String> sortedProductIds = products.stream()
                .sorted(Comparator.comparingInt(ProductSortOrder::getSortOrder))
                .map(ProductSortOrder::getProductId)
                .distinct()
                .collect(Collectors.toList());

        logger.info("sorted size :: {}, ids: {}", sortedProductIds.size(), sortedProductIds);

        List<ProductIds> productIds = productIdsRepository.findAllById(sortedProductIds);

        logger.info("sorted product id details size ::: {}, :: {}", productIds.size(), productIds);

        Map<String, String> productIdToShopifyIdMap = productIds.stream()
                .collect(Collectors.toMap(ProductIds::getProductId, ProductIds::getShopifyProductId));

        List<String> shopifyProductIds = sortedProductIds.stream()
                .map(productIdToShopifyIdMap::get) // Preserve original sort order
                .collect(Collectors.toList());

        logger.info("sorted shopify ids size :: {}, :: {}", shopifyProductIds.size(), shopifyProductIds);

        migrationService.addProductsToCollection(collectionId, shopifyProductIds);

        return "Products imported successfully.";
    }

    @PostMapping("/product-varient-migration")
    public String productVarientMigration() {
        migrationService.productVarientMigration();
        return "Products imported started successfully.";
    }

    @PostMapping("/imported-product-2-fields-re-upload")
    public String importedProduct2FieldReUpload() {
        migrationService.importedProduct2FieldReUpload();
        return "Imported Product 2 Field ReUpload started successfully";
    }
}
