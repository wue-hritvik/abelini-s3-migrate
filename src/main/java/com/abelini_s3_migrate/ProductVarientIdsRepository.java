package com.abelini_s3_migrate;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProductVarientIdsRepository extends JpaRepository<ProductVarientIds, Long> {
}
