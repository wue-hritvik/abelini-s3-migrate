package com.abelini_s3_migrate.controller;

import com.abelini_s3_migrate.repo.ProductIdsRepository;
import com.abelini_s3_migrate.service.ProductMigrationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/product/migrate")
public class ProductMigrateController {
    private static final Logger logger = LoggerFactory.getLogger(ProductMigrateController.class);
    @Autowired
    private ProductMigrationService migrationService;
    @Autowired
    private ProductIdsRepository productIdsRepository;

//    @PostMapping("/upload")
//    public ResponseEntity<String> uploadFile(@RequestParam(value = "file", required = false) MultipartFile file,
//                                             @RequestParam(defaultValue = "0", required = false) String id) {
//        try {
//            migrationService.processProducts(file, id);
//            return ResponseEntity.ok("Migration started successfully");
//        } catch (Exception e) {
//            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage());
//        }
//    }
//
//    @GetMapping("/missing")
//    public ResponseEntity<?> getMissingProducts() {
//        try {
//            return migrationService.getMissingProducts();
//        } catch (Exception e) {
//            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage());
//        }
//    }
//
//    @PostMapping("/import-product-collection")
//    public String importProductsToCollection(@RequestParam String collectionId, @RequestBody List<ProductSortOrder> products) {
//        String startTime = ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z"));
//        logger.info("Starting product import in collection... at:: {}", startTime);
//
//        logger.info("payload size :: {}", products.size());
//
//        List<String> sortedProductIds = products.stream()
//                .sorted(Comparator.comparingInt(ProductSortOrder::getSortOrder))
//                .map(ProductSortOrder::getProductId)
//                .distinct()
//                .collect(Collectors.toList());
//
//        logger.info("sorted size :: {}, ids: {}", sortedProductIds.size(), sortedProductIds);
//
//        List<ProductIds> productIds = productIdsRepository.findAllById(sortedProductIds);
//
//        logger.info("sorted product id details size ::: {}, :: {}", productIds.size(), productIds);
//
//        Map<String, String> productIdToShopifyIdMap = productIds.stream()
//                .collect(Collectors.toMap(ProductIds::getProductId, ProductIds::getShopifyProductId));
//
//        List<String> shopifyProductIds = sortedProductIds.stream()
//                .map(productIdToShopifyIdMap::get) // Preserve original sort order
//                .collect(Collectors.toList());
//
//        logger.info("sorted shopify ids size :: {}, :: {}", shopifyProductIds.size(), shopifyProductIds);
//
//        migrationService.addProductsToCollection(collectionId, shopifyProductIds);
//
//        return "Products imported successfully.";
//    }
//
//    @PostMapping("/product-varient-migration")
//    public String productVarientMigration(@RequestParam(required = false, defaultValue = "true") boolean isTest) {
//        migrationService.productVarientMigration(isTest);
//        return "Products varient imported started successfully.";
//    }
//
//    @PostMapping("/imported-product-2-fields-re-upload")
//    public String importedProduct2FieldReUpload() {
//        migrationService.importedProduct2FieldReUpload();
//        return "Imported Product 2 Field ReUpload started successfully";
//    }
//
//    @PostMapping("/imported-product-2-fields-re-upload/second-time")
//    public String importedProduct2FieldReUploadSecond() {
//        migrationService.importedProduct2FieldReUploadSecond();
//        return "Imported Product 2 Field second ReUpload started successfully";
//    }

    @PostMapping("/imported-2-lakh-product")
    public String imported2LakhProduct(@RequestParam(required = false, defaultValue = "true") boolean isTest) {
        migrationService.imported2LakhProduct(isTest);
        return "Imported 2 Lakh Product started successfully";
    }
}
