package com.bny.dw.kafkasnowflakeingest.controller;

import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestPart;
//import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.ResponseBody;

import com.bny.dw.kafkasnowflakeingest.service.FileStorageService;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

@OpenAPIDefinition(info = @Info(title = "Warehouse API", version = "1.0", description = "API documentation"))
@Controller
@RequiredArgsConstructor
public class FileUploadController {
    private final FileStorageService storageService;

    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @ResponseBody
    public Mono<String> upload(@RequestPart("file") FilePart file) {
        return storageService.store(file)
                .map(path -> "Uploaded to: " + path);
    }

    @GetMapping("/upload")
    public String uploadForm() {
        return "upload"; // upload.html in templates/
    }
}
