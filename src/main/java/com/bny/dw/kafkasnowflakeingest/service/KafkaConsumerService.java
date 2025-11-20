package com.bny.dw.kafkasnowflakeingest.service;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final SnowflakeService snowflakeService;
    private final ObjectMapper mapper = new ObjectMapper();

    public KafkaConsumerService(SnowflakeService snowflakeService) {
        this.snowflakeService = snowflakeService;
    }

    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.group.id}")
    public void listen(String message) {
        log.info("Consumed message: {}", message);

        try {
            JsonNode node = mapper.readTree(message);

            String tradeDateStr = node.get("trade_date").asText();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy");
            LocalDate tradeDate = LocalDate.parse(tradeDateStr, formatter);

            Timestamp ts = Timestamp.valueOf(tradeDate.atStartOfDay());

            snowflakeService.insertTransaction(
                    node.get("trade_id").asText(),
                    node.get("trader").asText(),
                    node.get("price").asDouble(),
                    node.get("quantity").asInt(),
                    node.get("fund").asText(),
                    node.get("security").asText(),
                    ts);

            log.info("Inserted transaction id={}", node.get("trade_id").asText());
        } catch (Exception e) {
            log.error("Failed to process message", e);
        }
    }
}