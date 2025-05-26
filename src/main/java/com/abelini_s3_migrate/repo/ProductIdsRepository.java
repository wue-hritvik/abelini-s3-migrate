package com.abelini_s3_migrate.repo;

import com.abelini_s3_migrate.entity.ProductIds;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProductIdsRepository extends JpaRepository<ProductIds, String> {
    List<ProductIds> findByProductId(String number);
}
