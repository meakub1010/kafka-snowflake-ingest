package com.bny.dw.kafkasnowflakeingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class KafkaSnowflakeIngestApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaSnowflakeIngestApplication.class, args);
	}

}
