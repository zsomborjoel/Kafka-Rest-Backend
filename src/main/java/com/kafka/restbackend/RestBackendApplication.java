package com.kafka.restbackend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class RestBackendApplication {

	public static void main(String[] args) {
		SpringApplication.run(RestBackendApplication.class, args);
	}

}
