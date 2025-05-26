package com.abelini_s3_migrate.extra;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProductSortOrder {
    @JsonProperty("product_id")
    private String productId;

    @JsonProperty("sort_order")
    private int sortOrder;

    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }
    public int getSortOrder() { return sortOrder; }
    public void setSortOrder(int sortOrder) { this.sortOrder = sortOrder; }
}
