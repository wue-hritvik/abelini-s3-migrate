package com.abelini_s3_migrate;

import com.opencsv.CSVWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class S3Service {
    private static final Logger logger = LoggerFactory.getLogger(S3Service.class);

    @Value("${aws.s3.bucket}")
    private String bucketName;

    @Value("${aws.s3.region}")
    private String region;

    @Value("${aws.accessKeyId}")
    private String accessKey;

    @Value("${aws.secretAccessKey}")
    private String secretKey;

    public String exportS3ImagesToCSV() {
        logger.info("Fetching all image URLs from S3...");

        S3Client s3 = S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .build();

        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .delimiter("") // ✅ Ensures all files, including subfolders, are retrieved
                .build();

        ListObjectsV2Response result = s3.listObjectsV2(request);
        List<String[]> csvData = new ArrayList<>();
        csvData.add(new String[]{"image_url"}); // Header

        for (S3Object object : result.contents()) {
            String url = "https://" + bucketName + ".s3." + region + ".amazonaws.com/" + object.key();
            csvData.add(new String[]{url});
        }

        String filePath = "src/main/resources/s3file/s3_images.csv";

        try (CSVWriter writer = new CSVWriter(new FileWriter(filePath))) {
            writer.writeAll(csvData);
            logger.info("✅ CSV file created: {}", filePath);
        } catch (IOException e) {
            logger.error("❌ Error writing CSV file: {}", e.getMessage());
            return "Error writing CSV file!";
        }

        return filePath;
    }
}
