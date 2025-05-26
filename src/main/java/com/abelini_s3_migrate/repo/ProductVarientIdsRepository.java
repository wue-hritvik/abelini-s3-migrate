package com.abelini_s3_migrate.repo;

import com.abelini_s3_migrate.entity.ProductVarientIds;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProductVarientIdsRepository extends JpaRepository<ProductVarientIds, Long> {
}
