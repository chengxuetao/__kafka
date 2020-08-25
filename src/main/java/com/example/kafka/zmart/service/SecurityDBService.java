package com.example.kafka.zmart.service;

import java.util.Date;

public class SecurityDBService {

    public static void saveRecord(Date purchaseDate, String employeeId, String itemPurchased) {
        System.out.println("saveRecord : purchaseDate=" + purchaseDate + ", employeeId=" + employeeId
            + ", itemPurchased" + itemPurchased);
    }

}
