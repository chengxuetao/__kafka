package com.example.kafka.zmart.model;

import java.util.Date;
import java.util.Objects;

public class Purchase {

    private String employeeId;
    private String firstName;
    private String lastName;
    private String creditCardNumber;
    private String itemPurchased;
    int quantity;
    double price;
    private Date purchaseDate;
    private String zipCode;
    private String department;

    private Purchase(Builder builder) {
        employeeId = builder.employeeId;
        firstName = builder.firstName;
        lastName = builder.lastName;
        creditCardNumber = builder.creditCardNumber;
        itemPurchased = builder.itemPurchased;
        quantity = builder.quanity;
        price = builder.price;
        purchaseDate = builder.purchaseDate;
        zipCode = builder.zipCode;
        department = builder.department;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Purchase copy) {
        Builder builder = new Builder();
        builder.employeeId = copy.employeeId;
        builder.firstName = copy.firstName;
        builder.lastName = copy.lastName;
        builder.creditCardNumber = copy.creditCardNumber;
        builder.itemPurchased = copy.itemPurchased;
        builder.quanity = copy.quantity;
        builder.price = copy.price;
        builder.purchaseDate = copy.purchaseDate;
        builder.zipCode = copy.zipCode;
        builder.department = copy.department;
        return builder;
    }

    public String getEmployeeId() {
        return employeeId;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getCreditCardNumber() {
        return creditCardNumber;
    }

    public String getItemPurchased() {
        return itemPurchased;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getPrice() {
        return price;
    }

    public Date getPurchaseDate() {
        return purchaseDate;
    }

    public String getZipCode() {
        return zipCode;
    }

    public String getDepartment() {
        return department;
    }

    @Override
    public String toString() {
        return "Purchase [employeeId=" + employeeId + ", firstName=" + firstName + ", lastName=" + lastName
            + ", creditCardNumber=" + creditCardNumber + ", itemPurchased=" + itemPurchased + ", quantity=" + quantity
            + ", price=" + price + ", purchaseDate=" + purchaseDate + ", zipCode=" + zipCode + ", department="
            + department + "]";
    }

    public static final class Builder {
        private String employeeId;
        private String firstName;
        private String lastName;
        private String creditCardNumber;
        private String itemPurchased;
        private int quanity;
        private double price;
        private Date purchaseDate;
        private String zipCode;
        private String department;

        private static final String CC_NUMBER_REPLACEMENT = "xxxx-xxxx-xxxx-";

        private Builder() {
        }

        public Builder employeeId(String val) {
            employeeId = val;
            return this;
        }

        public Builder firstName(String val) {
            firstName = val;
            return this;
        }

        public Builder lastName(String val) {
            lastName = val;
            return this;
        }

        public Builder maskCreditCard() {
            Objects.requireNonNull(this.creditCardNumber, "Credit Card can't be null");
            String last4Digits = this.creditCardNumber.split("-")[3];
            this.creditCardNumber = CC_NUMBER_REPLACEMENT + last4Digits;
            return this;
        }

        public Builder creditCardNumber(String val) {
            creditCardNumber = val;
            return this;
        }

        public Builder itemPurchased(String val) {
            itemPurchased = val;
            return this;
        }

        public Builder quanity(int val) {
            quanity = val;
            return this;
        }

        public Builder price(double val) {
            price = val;
            return this;
        }

        public Builder purchaseDate(Date val) {
            purchaseDate = val;
            return this;
        }

        public Builder zipCode(String val) {
            zipCode = val;
            return this;
        }

        public Builder department(String val) {
            department = val;
            return this;
        }

        public Purchase build() {
            return new Purchase(this);
        }
    }
}
