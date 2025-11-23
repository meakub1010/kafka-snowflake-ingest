package com.bny.dw.kafkasnowflakeingest.service;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class FileProcessorService {
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Async
    public void processFileAsync(Path path) {
        log.info("Processing file at: {}", path);

        DateTimeFormatter dateFmt = DateTimeFormatter.ofPattern("M/d/yyyy");

        try (BufferedReader br = Files.newBufferedReader(path)) {
            br.readLine(); // skip header

            String line;
            while ((line = br.readLine()) != null) {
                line = line.strip();
                if (line.isEmpty())
                    continue;

                String[] c = line.split(",");
                if (c.length < 6) {
                    log.warn("Skipping malformed line (not enough columns): {}", line);
                    continue;
                }

                String tradeDateStr = c[0].trim();
                String fund = c[1].trim();
                String trader = c[2].trim();
                String security = c[3].trim();
                String qtyStr = c[4].trim();
                String priceStr = c[5].trim();

                LocalDate tradeDate;
                try {
                    tradeDate = LocalDate.parse(tradeDateStr, dateFmt);
                } catch (Exception e) {
                    log.warn("Skipping line due to bad trade date [{}]: {}", tradeDateStr, line);
                    continue;
                }

                Double quantity;
                try {
                    quantity = Double.parseDouble(qtyStr);
                } catch (Exception e) {
                    log.warn("Skipping line due to invalid quantity [{}]: {}", qtyStr, line);
                    continue;
                }

                Double price;
                try {
                    price = Double.parseDouble(priceStr);
                } catch (Exception e) {
                    log.warn("Skipping line due to invalid price [{}]: {}", priceStr, line);
                    continue;
                }
                if (fund.isEmpty() || trader.isEmpty() || security.isEmpty()) {
                    log.warn("Skipping line due to empty required field: {}", line);
                    continue;
                }
                UUID tradeId = UUID.randomUUID();
                String json = String.format(
                        "{ \"trade_id\": \"%s\", \"trade_date\": \"%s\", \"fund\": \"%s\", \"trader\": \"%s\", \"security\": \"%s\", \"quantity\": %s, \"price\": %s }",
                        tradeId,
                        tradeDate.format(dateFmt),
                        fund,
                        trader,
                        security,
                        quantity,
                        price);

                CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("transactions",
                        tradeId.toString(), json);
                future.whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Kafka producer error", ex);
                    } else {
                        log.info("sent to kafka with offet {} to partition {}", result.getRecordMetadata().offset(),
                                result.getRecordMetadata().partition());
                    }
                });

            }

            kafkaTemplate.flush();

            log.info("Finished processing file {}", path);

        } catch (Exception e) {
            log.error("Error processing file {}", path, e);
        }

    }
}
