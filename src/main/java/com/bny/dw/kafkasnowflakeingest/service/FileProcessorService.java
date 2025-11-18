package com.bny.dw.kafkasnowflakeingest.service;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.springframework.kafka.core.KafkaTemplate;
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
        try (BufferedReader br = Files.newBufferedReader(path)) {
            br.readLine(); // skip header

            String line;
            while ((line = br.readLine()) != null) {
                line = line.strip();
                if (line.isEmpty()) {
                    continue;
                }
                String[] c = line.split(",");
                if (c.length < 6) {
                    log.warn("Skipping malformed line: {}", line);
                    continue;
                }
                // convert CSV row to JSON for Snowflake Sink
                String json = String.format(
                        "{\"trade_id\":\"%s\", \"trade_date\":\"%s\", \"fund\":\"%s\", \"trader\":\"%s\", \"security\":\"%s\", \"price\":%s, \"quantity\":%s}",
                        UUID.randomUUID(),
                        c[0], c[1], c[2], c[3], c[4], c[5]);

                kafkaTemplate.send("transactions", json);
            }

            log.info("Finished processing file {}", path);

        } catch (Exception e) {
            log.error("Error processing file", e);
        }
    }
}
