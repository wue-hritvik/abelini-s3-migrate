package com.abelini_s3_migrate;

public class ProductEntry {
    private final String productId;
    private final String tagNo;
    private final int sortOrder;

    public ProductEntry(String productId, String tagNo, int sortOrder) {
        this.productId = productId;
        this.tagNo = tagNo;
        this.sortOrder = sortOrder;
    }

    public String getProductId() { return productId; }
    public String getTagNo() { return tagNo; }
    public int getSortOrder() { return sortOrder; }
}
