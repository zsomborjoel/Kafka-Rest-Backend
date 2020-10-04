package com.kafka.restbackend.service;

import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.restbackend.config.KafkaConfig;
import com.kafka.restbackend.model.Customer;
import com.kafka.restbackend.repository.RocksDbRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class CustomerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomerService.class);

    private static final String CUSTOMER = "customer";

    @Value("${kafka.topic.customer}")
    private String customerTopic;

    @Autowired
    private KafkaConfig kafkaConfig;

    @Autowired
    private RocksDbRepository rocksDbRepository;

    @Autowired
    private ObjectMapper objectMapper;

    public String sendMessage(Customer customer) {
        String key = CUSTOMER + customer.getKey();
        String message = getMessage(customer);
        LOGGER.info(String.format("Producing key: %s, message: %s", key, message));

        ListenableFuture<SendResult<String, String>> future = kafkaConfig.kafkaTemplate().send(customerTopic, key, message);
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                LOGGER.info("Unable to send key=[ {} ] due to : {}", key, ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                LOGGER.info("Sent key=[ {} ] with offset=[ {} ]", key, result.getRecordMetadata().offset());
                rocksDbRepository.save(key, message);
                LOGGER.info("Customer saved to rocksDb");
            }
        });
        return key;
    }

    public Optional<Customer> findById(String key) {
        Optional<Customer> customer = Optional.of(new Customer());
        String customerData = rocksDbRepository.find(key);
        try {
            if (customerData != null) {
                customer = Optional.of(objectMapper.readValue(customerData, Customer.class));
            } else {
                LOGGER.warn("Rocksdb Customer data is null.");
            }
        } catch (JsonProcessingException e) {
            LOGGER.error("Customer class deserialization exception occured : ", e);
        }
        return customer;
    }

    private String getMessage(Customer customer) {
        String message = null;
        try {
            message = objectMapper.writeValueAsString(customer);
        } catch (JsonProcessingException e) {
            LOGGER.error("Customer class serialization exception occured : ", e);
        }
        return message;
    }


}