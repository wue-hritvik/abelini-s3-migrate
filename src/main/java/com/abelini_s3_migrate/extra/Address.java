package com.abelini_s3_migrate.extra;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Address {
    private String firstName;
    private String lastName;
    private String company;
    private String address1;
    private String address2;
    private String city;
    private String province;
    private String country;
    private String zip;
    private String phone;
    @JsonProperty("isDefault")
    private boolean isDefault;
}
