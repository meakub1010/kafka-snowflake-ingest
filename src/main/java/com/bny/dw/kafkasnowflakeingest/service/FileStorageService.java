package com.bny.dw.kafkasnowflakeingest.service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;

import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class FileStorageService {
    private final FileProcessorService processor;

    public Mono<Path> store(FilePart file) {
        LocalDate today = LocalDate.now();
        Path folder = Paths.get("data",
                String.valueOf(today.getYear()),
                String.valueOf(today.getMonthValue()),
                String.valueOf(today.getDayOfMonth()));

        try {
            Files.createDirectories(folder);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        Path dest = folder.resolve(file.filename());
        return file.transferTo(dest)
                .then(Mono.fromRunnable(() -> {
                    processor.processFileAsync(dest);
                }))
                .thenReturn(dest);
    }
}
