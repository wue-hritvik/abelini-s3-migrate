package com.abelini_s3_migrate.repo;

import com.abelini_s3_migrate.entity.ProductCarat;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Set;

@Repository
public interface ProductCaratRepository extends JpaRepository<ProductCarat, Long> {
    @Query("SELECT p.variantCode FROM ProductCarat p WHERE p.productId = :productId")
    Set<String> findVarientIdsByProductId(@Param("productId") String productId);
}
