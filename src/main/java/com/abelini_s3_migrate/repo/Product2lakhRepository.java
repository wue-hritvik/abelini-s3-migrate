package com.abelini_s3_migrate.repo;

import com.abelini_s3_migrate.entity.Product2Lakh;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface Product2lakhRepository extends JpaRepository<Product2Lakh, Long> {
}
