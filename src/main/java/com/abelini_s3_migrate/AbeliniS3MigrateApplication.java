package com.abelini_s3_migrate;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class AbeliniS3MigrateApplication {

	public static void main(String[] args) {
		SpringApplication.run(AbeliniS3MigrateApplication.class, args);
	}

}
