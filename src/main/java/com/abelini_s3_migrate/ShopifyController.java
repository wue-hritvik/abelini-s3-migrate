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
import java.util.Objects;

@RestController
@RequestMapping("/shopify")
public class ShopifyController {
    private final S3Service s3Service;
    private final ShopifyService shopifyService;

    public ShopifyController(S3Service s3Service, ShopifyService shopifyService) {
        this.s3Service = s3Service;
        this.shopifyService = shopifyService;
    }

    @PostMapping("/3/migrate")
    public String migrateImages(@RequestParam(required = false) String path) {
        String csvPath;
        csvPath = Objects.requireNonNullElse(path, "src/main/resources/s3file/s3_url_list.csv");
        try {
            shopifyService.uploadImagesToShopify(csvPath);
            return "Migration started!";
        } catch (IOException | CsvException e) {
            return "Error: " + e.getMessage();
        }
    }

    @PostMapping("s3upload")
    public String s3Upload(@RequestBody String path) {
        try {
            return shopifyService.uploadFileToShopify(path);
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    @PostMapping("/2/generate-csv")
    public String generateCsv(@RequestParam(required = false) String fileName,
                              @RequestParam(defaultValue = "false") boolean onlySupportedFile) {
        String name;
        name = Objects.requireNonNullElse(fileName, "s3_url_list");
        return s3Service.exportS3ImagesToCSV(name, onlySupportedFile);
    }

    @GetMapping("/1/rename-files")
    public String renameFiles() {
        s3Service.renameAndCopyFiles();
        return "Bulk file renaming and copying started!";
    }

    @GetMapping("/download-csv")
    public ResponseEntity<?> downloadCsvFile(@RequestParam(required = false) String path) {
        String csvPath;
        csvPath = Objects.requireNonNullElse(path, "src/main/resources/s3file/s3_url_list.csv");
        File file = new File(csvPath);

        if (!file.exists()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("s3 file not found");
        }

        Resource fileResource = new FileSystemResource(file);
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("text/csv"))
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + file.getName())
                .body(fileResource);
    }

    @GetMapping("/test")
    public String test() {
        return "success";
    }

}