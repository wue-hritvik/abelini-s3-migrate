package com.abelini_s3_migrate.repo;

import com.abelini_s3_migrate.entity.Product2Lakh;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.*;

@Repository
public interface Product2lakhRepository extends JpaRepository<Product2Lakh, Long> {
    @Query("SELECT p.variantCode FROM Product2Lakh p WHERE p.productId = :productId")
    Set<String> findVarientIdsByProductId(@Param("productId") String productId);
}
