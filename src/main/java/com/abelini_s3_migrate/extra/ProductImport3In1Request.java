package com.abelini_s3_migrate.extra;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductImport3In1Request {
    private Set<Long> searchFailedProductIds;
    private Set<Long> caratFailedProductIds;
    private Set<Long> bestsellerFailedProductIds;
}
