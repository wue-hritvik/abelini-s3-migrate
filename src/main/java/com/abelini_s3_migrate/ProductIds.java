package com.abelini_s3_migrate;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

@Getter
@Setter
@Entity
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "product_ids")
public class ProductIds {

    @Id
    private String productId;

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
}
