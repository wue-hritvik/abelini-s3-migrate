package com.abelini_s3_migrate.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Entity
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "dd_product_to_shopify_stock")
public class ProductVarientIds {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String productId;

    private String tagNo;

    private String shopifyProductId;

    public void setProductId(String productId) {
        this.productId = productId;
    }
    public void setShopifyProductId(String shopifyProductId) {
        this.shopifyProductId = shopifyProductId;
    }

    public String getProductId() {
        return productId;
    }

    public String getShopifyProductId() {
        return shopifyProductId;
    }

    @Override
    public String toString() {
        return "ProductIds{id='"+id+"', productId='" + productId + "', tagNo='" + tagNo + "', shopifyProductId='" + shopifyProductId + "'}";
    }
}
