package com.abelini_s3_migrate.controller;

import com.abelini_s3_migrate.service.ProductMigrationService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class RateController {

    private final ProductMigrationService productMigrationService;

    public RateController(ProductMigrationService productMigrationService) {
        this.productMigrationService = productMigrationService;
    }

    @GetMapping("api/v1/rate-limit/consume")
    public ResponseEntity<?> apiRateLimitConsume() {
        try {
            productMigrationService.regulateApiRate();
            ProductMigrationService.remainingPoints.addAndGet(-ProductMigrationService.API_COST_PER_CALL);
            return ResponseEntity.ok("Allowed");
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error occurred or Rate limit too low. Try again later.");
        }
    }

    @GetMapping("api/v1/rate-limit/check")
    public ResponseEntity<?> apiRateLimitCheck() {
        try {
            return ResponseEntity.ok(ProductMigrationService.remainingPoints.get());
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error occurred, Try again later.");
        }
    }

    @GetMapping("api/v1/rate-limit/refresh")
    public ResponseEntity<?> apiRateLimitRefresh() {
        try {
            productMigrationService.initializeRemainingPointsFromShopify();
            return ResponseEntity.ok("Refreshed Successfully");
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error occurred while refreshing rate limit, Try again later.");
        }
    }
}
