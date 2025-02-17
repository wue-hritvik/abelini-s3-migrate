package com.abelini_s3_migrate;

import com.opencsv.CSVWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@Service
public class S3Service {
    private static final Logger logger = LoggerFactory.getLogger(S3Service.class);

    @Value("${aws_s3_bucket}")
    private String bucketName;

    @Value("${aws_s3_region}")
    private String region;

    @Value("${aws_access_key}")
    private String accessKey;

    @Value("${aws_secret_key}")
    private String secretKey;
    private final Executor executor;

    public S3Service(@Qualifier("s3TaskExecutor") Executor executor) {

        this.executor = executor;
    }

    @Async
    public void exportS3ImagesToCSV(String name, boolean onlySupportedFile) {
        logger.info("Fetching all image URLs from S3 ...");

        S3Client s3 = S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .build();

        // File path for saving CSV
        String filePath = "src/main/resources/s3file/" + name + ".csv";

        try (CSVWriter writer = new CSVWriter(new FileWriter(filePath))) {
            // Write CSV header
            writer.writeNext(new String[]{"image_url"});

            String continuationToken = null;

            do {
                ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .maxKeys(1000); // Fetch in batches

                if (continuationToken != null) {
                    requestBuilder.continuationToken(continuationToken);
                }

                ListObjectsV2Response result = s3.listObjectsV2(requestBuilder.build());

                for (S3Object object : result.contents()) {
                    String url = "https://" + bucketName + ".s3." + region + ".amazonaws.com/" + object.key();
                    writer.writeNext(new String[]{url});
                }

                // Optional: flush to disk after processing a batch
                writer.flush();

                continuationToken = result.nextContinuationToken();
            } while (continuationToken != null);

            logger.info("✅ CSV file created: {}", filePath);
        } catch (IOException e) {
            logger.error("❌ Error writing CSV file: {}", e.getMessage());
        }

        logger.info("CSV file generation completed.");
    }


    public void renameAndCopyFiles() {

        S3Client s3 = S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .build();

        String continuationToken = null;
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        do {
            // Fetch files from S3 using AWS SDK v2
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .maxKeys(1000); // Adjust batch size for better performance

            if (continuationToken != null) {
                requestBuilder.continuationToken(continuationToken);
            }

            ListObjectsV2Response response = s3.listObjectsV2(requestBuilder.build());

            for (S3Object s3Object : response.contents()) {
                String originalKey = s3Object.key();

                // Skip files in rename_files/ folder
                if (originalKey.startsWith("rename_files/")) {
                    continue;
                }

                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        String newKey = "rename_files/" + originalKey.replace("/", "_");

                        // Copy the file to the new location
                        CopyObjectRequest copyRequest = CopyObjectRequest.builder()
                                .sourceBucket(bucketName)
                                .sourceKey(originalKey)
                                .destinationBucket(bucketName)
                                .destinationKey(newKey)
                                .build();

                        s3.copyObject(copyRequest);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, executor);

                futures.add(future);
            }

            // Check if more objects exist in S3
            continuationToken = response.nextContinuationToken();

        } while (continuationToken != null);

        // Wait for all tasks to finish
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }
}
