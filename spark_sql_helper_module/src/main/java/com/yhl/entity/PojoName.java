package com.yhl.entity;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PojoName implements Entity,Serializable {
        public String BUSI_PROD_CODE;
        public String BUSI_PROD_NAME;
        public String DERIV_TYPE;
    }