package com.kafka.restbackend.model;

import java.util.Objects;

public class Customer {
    
    String key;
    String name;
    String phoneNumber;
    String email;
    String address;

    public Customer() {
    }

    public Customer(String key, String name, String phoneNumber, String email, String address) {
        this.key = key;
        this.name = name;
        this.phoneNumber = phoneNumber;
        this.email = email;
        this.address = address;
    }

    public String getKey() {
        return this.key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPhoneNumber() {
        return this.phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getEmail() {
        return this.email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getAddress() {
        return this.address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Customer key(String key) {
        this.key = key;
        return this;
    }

    public Customer name(String name) {
        this.name = name;
        return this;
    }

    public Customer phoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
        return this;
    }

    public Customer email(String email) {
        this.email = email;
        return this;
    }

    public Customer address(String address) {
        this.address = address;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Customer)) {
            return false;
        }
        Customer customer = (Customer) o;
        return Objects.equals(key, customer.key) && Objects.equals(name, customer.name) && Objects.equals(phoneNumber, customer.phoneNumber) && Objects.equals(email, customer.email) && Objects.equals(address, customer.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, name, phoneNumber, email, address);
    }

    @Override
    public String toString() {
        return "{" +
            " key='" + getKey() + "'" +
            ", name='" + getName() + "'" +
            ", phoneNumber='" + getPhoneNumber() + "'" +
            ", email='" + getEmail() + "'" +
            ", address='" + getAddress() + "'" +
            "}";
    }


}
