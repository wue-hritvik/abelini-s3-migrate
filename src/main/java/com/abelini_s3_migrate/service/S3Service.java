package com.abelini_s3_migrate.service;

import com.opencsv.CSVWriter;
import org.apache.tika.Tika;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class S3Service {
    private static final Logger logger = LoggerFactory.getLogger(S3Service.class);

//    @Value("${aws_s3_bucket}")
//    private String bucketName;

    @Value("${aws_s3_region}")
    private String region;

    @Value("${aws_access_key}")
    private String accessKey;

    @Value("${aws_secret_key}")
    private String secretKey;
    private final Executor executor;
    private final Tika tika;

    public S3Service(@Qualifier("s3TaskExecutor") Executor executor, Tika tika) {

        this.executor = executor;
        this.tika = tika;
    }

    @Async
    public void exportS3ImagesToCSV(String name, boolean onlySupportedFile, String bucketName) {
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
                    String key = object.key();
                    if (key.equals("file_s3_batch_operation.csv") || key.equals("rename-manifest.csv")) {
                        continue;
                    }
                    String url = "https://" + bucketName + ".s3." + region + ".amazonaws.com/" + key;
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


//    public void renameAndCopyFiles() {
//
//        S3Client s3 = S3Client.builder()
//                .region(Region.of(region))
//                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
//                .build();
//
//        String continuationToken = null;
//        List<CompletableFuture<Void>> futures = new ArrayList<>();
//
//        do {
//            // Fetch files from S3 using AWS SDK v2
//            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
//                    .bucket(bucketName)
//                    .maxKeys(1000); // Adjust batch size for better performance
//
//            if (continuationToken != null) {
//                requestBuilder.continuationToken(continuationToken);
//            }
//
//            ListObjectsV2Response response = s3.listObjectsV2(requestBuilder.build());
//
//            for (S3Object s3Object : response.contents()) {
//                String originalKey = s3Object.key();
//
//                // Skip files in rename_files/ folder
//                if (originalKey.startsWith("rename_files/")) {
//                    continue;
//                }
//
//                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
//                    try {
//                        String newKey = "rename_files/" + originalKey.replace("/", "_");
//
//                        // Copy the file to the new location
//                        CopyObjectRequest copyRequest = CopyObjectRequest.builder()
//                                .sourceBucket(bucketName)
//                                .sourceKey(originalKey)
//                                .destinationBucket(bucketName)
//                                .destinationKey(newKey)
//                                .build();
//
//                        s3.copyObject(copyRequest);
//
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }, executor);
//
//                futures.add(future);
//            }
//
//            // Check if more objects exist in S3
//            continuationToken = response.nextContinuationToken();
//
//        } while (continuationToken != null);
//
//        // Wait for all tasks to finish
//        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
//    }

    @Async
    public void renameAndCopyFiles() {
        String csvPath = "src/main/resources/s3file/missing_links_all_14-07-25.csv";
        String destinationBucket = "renamed-object-till-14-july-25";

        logger.info("Starting rename file names started at :: {}", ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z")));

        AtomicLong processed = new AtomicLong(0);
//        AtomicLong total = new AtomicLong(0);

        S3Client s3Client = S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .build();

        try (BufferedReader reader = new BufferedReader(new FileReader(csvPath))) {
            String header = reader.readLine(); // Skip header
            String line;

            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;

                String s3Url = line.replaceAll("\"", "").trim(); // clean quotes

                if (notSupportedFileType(s3Url)) continue;

                URL url = new URL(s3Url);

                String host = url.getHost(); // e.g., abelini-images.s3.eu-west-2.amazonaws.com
                String originalBucket = host.split("\\.")[0]; // abelini-images
                String originalKey = url.getPath().substring(1); // remove leading slash

                // Generate new key by replacing '/' with '_'
                String newKey = originalKey.replace("/", "_");

                // Build CopyObjectRequest
                CopyObjectRequest copyRequest = CopyObjectRequest.builder()
                        .copySource(originalBucket + "/" + originalKey)
                        .destinationBucket(destinationBucket)
                        .destinationKey(newKey)
                        .build();

                s3Client.copyObject(copyRequest);
                System.out.println("Copied: " + originalKey + " ➝ " + newKey);
                System.out.println("Processed till now: " + processed.incrementAndGet());
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Failed: " + e.getMessage());
        }
        logger.info("Completed file renames... ended at :: {}", ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("dd MM yyyy hh:mm:ss a z")));
        logger.info("Optimized renaming and copying to '" + destinationBucket + "' completed!");
    }

    private static final Set<String> SUPPORTED_IMAGE_MIME_TYPES = Set.of(
//            "image/png", "image/jpeg", "image/gif", "image/jpg", "image/webp", "image/svg+xml"
//            ,
            "image/avif", "video/mp4"
    );

    private boolean notSupportedFileType(String fileUrl) {
        String mimeType = detectMimeType(fileUrl);
        return !SUPPORTED_IMAGE_MIME_TYPES.contains(mimeType);
    }

    private String detectMimeType(String filename) {
        try {
            return tika.detect(filename);
        } catch (Exception e) {
            logger.warn("Could not detect MIME type for {}. Defaulting to image/jpeg", filename);
            return "image/jpeg";
        }
    }
}
