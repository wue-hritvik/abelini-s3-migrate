package com.abelini_s3_migrate.controller;


import com.abelini_s3_migrate.service.ShopifyFileFetcherService;
import com.abelini_s3_migrate.service.S3Service;
import com.abelini_s3_migrate.service.ShopifyService;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

@RestController
@RequestMapping("/shopify")
public class ShopifyController {
    private final S3Service s3Service;
    private final ShopifyService shopifyService;
    private final ShopifyFileFetcherService shopifyFileFetcherService;

    public ShopifyController(S3Service s3Service, ShopifyService shopifyService, ShopifyFileFetcherService shopifyFileFetcherService) {
        this.s3Service = s3Service;
        this.shopifyService = shopifyService;
        this.shopifyFileFetcherService = shopifyFileFetcherService;
    }

//    @PostMapping("/3/migrate")
//    public String migrateImages(@RequestParam(required = false) String path,
//                                @RequestParam(required = false, defaultValue = "true") boolean isFailed,
//                                @RequestBody Set<Integer> failedBatch) {
////        String csvPath;
////        if (path == null) {
////            csvPath = "src/main/resources/s3file/s3_url_list.csv";
////        } else {
////            csvPath = "src/main/resources/s3file/" + path.replace(".csv", "") + ".csv";
////        }
//        try {
//            shopifyService.uploadImagesToShopify(path, failedBatch, isFailed);
//            return "Migration started!";
//        } catch (Exception e) {
//            return "Error: " + e.getMessage();
//        }
//    }
//
//    @GetMapping("/image/import/summary")
//    public ResponseEntity<String> getImportSummaries() {
//        String summary = shopifyService.printSummary();
//
//        return ResponseEntity.ok()
//                .contentType(MediaType.TEXT_PLAIN)
//                .body(summary);
//    }

    ////    @PostMapping("s3upload")
////    public String s3Upload(@RequestBody String path) {
////        try {
////            return shopifyService.uploadFileToShopify(path);
////        } catch (Exception e) {
////            return "Error: " + e.getMessage();
////        }
////    }
//
//    @PostMapping("/2/generate-csv")
//    public String generateCsv(@RequestParam(required = false) String fileName,
//                              @RequestParam(defaultValue = "false") boolean onlySupportedFile,
//                              @RequestParam(defaultValue = "abelini-images", required = false) String bucketName) {
//        String name;
//        name = Objects.requireNonNullElse(fileName.replace(".csv", ""), UUID.randomUUID().toString());
//        s3Service.exportS3ImagesToCSV(name, onlySupportedFile, bucketName);
//        return "CSV file generation started! and fileName will be: " + name + ".csv";
//    }

//    @GetMapping("/1/rename-files")
//    public String renameFiles() {
//        s3Service.renameAndCopyFiles();
//        return "Bulk file renaming and copying started!";
//    }

    @GetMapping("/download-csv")
    public ResponseEntity<?> downloadCsvFile(@RequestParam String path) {
//        String csvPath;
//        if (path == null) {
//            csvPath = "src/main/resources/s3file/s3_url_list.csv";
//        } else {
//            csvPath = "src/main/resources/s3file/" + path.replace(".csv", "") + ".csv";
//        }
        File file = new File(path);

        if (!file.exists()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("s3 file not found");
        }

        Resource fileResource = new FileSystemResource(file);
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("text/csv"))
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + file.getName())
                .body(fileResource);
    }

    //
//    @GetMapping("/test")
//    public String test() {
//        return "success";
//    }
//
//    @GetMapping("export/file-names")
//    public String exportFileNamesFromShopify() {
//        shopifyFileFetcherService.fetchAndStoreShopifyFiles();
//        return "export file names from shopify started";
//    }

    //
//    @GetMapping("export/file-names/bulk")
//    public String exportFileNamesFromShopifyBulk(){
//        shopifyFileFetcherService.fetchAndStoreShopifyFilesBulk();
//        return "export file names from shopify bulk started";
//    }
//
//    @GetMapping("compare/files-names")
//    public String compareFileNames() {
//        shopifyFileFetcherService.compareFileNames();
//        return "compare file names started";
//    }

}