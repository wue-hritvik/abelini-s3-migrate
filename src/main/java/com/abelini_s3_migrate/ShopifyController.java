package com.abelini_s3_migrate;


import com.opencsv.exceptions.CsvException;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;

@RestController
@RequestMapping("/shopify")
public class ShopifyController {
    private final S3Service s3Service;
    private final ShopifyService shopifyService;

    public ShopifyController(S3Service s3Service, ShopifyService shopifyService) {
        this.s3Service = s3Service;
        this.shopifyService = shopifyService;
    }

    @PostMapping("/migrate")
    public String migrateImages() {
        String csvPath = s3Service.exportS3ImagesToCSV();
//        String csvPath= "src/main/resources/s3file/s3_images.csv";
        try {
            shopifyService.uploadImagesToShopify(csvPath);
            return "Migration started!";
        } catch (IOException | CsvException e) {
            return "Error: " + e.getMessage();
        }
    }

//    @PostMapping("/process-csv")
//    public String processCsv() {
////        String csvPath = s3Service.exportS3ImagesToCSV();
//        String csvPath= "src/main/resources/s3file/s3_images.csv";
//        try {
//            shopifyService.processCsv(csvPath);
//            return "Migration started!";
//        } catch (Exception e) {
//            return "Error: " + e.getMessage();
//        }
//    }

    @GetMapping("/download-csv")
    public ResponseEntity<?> downloadCsvFile() {
        String filePath = "src/main/resources/s3file/s3_images.csv";
        File file = new File(filePath);

        if (!file.exists()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("s3 file not found");
        }

        Resource fileResource = new FileSystemResource(file);
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("text/csv"))
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + file.getName())
                .body(fileResource);
    }

//    @GetMapping("/check-status")
//    public String checkMigrationStatus() {
//        return shopifyService.checkMigrationStatus();
//    }

}