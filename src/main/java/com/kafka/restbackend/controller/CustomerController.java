package com.kafka.restbackend.controller;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

import com.kafka.restbackend.model.Customer;
import com.kafka.restbackend.service.CustomerService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class CustomerController {
    
    @Autowired
    private CustomerService customerService;

    @GetMapping("/customers/{id}")
    ResponseEntity<?> getCustomerById(@PathVariable String id) {
        Optional<Customer> customer = customerService.findById(id);
        return customer.map(response -> ResponseEntity.ok(response))
                .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    @PostMapping("/customers")
    ResponseEntity<String> createCategory(@Validated @RequestBody Customer customer) throws URISyntaxException {
        String key = customerService.sendMessage(customer);
        return ResponseEntity.created(new URI("/api/customers/" + key)).body(key);
    }

}
