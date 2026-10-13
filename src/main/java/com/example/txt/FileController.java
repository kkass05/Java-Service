package com.example.txt;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/files")
@Profile("api")
public class FileController {
    private final KafkaTemplate<String, byte[]> kafka;
    private final FileStorage storage;
    private final String topic;
    public FileController(KafkaTemplate<String, byte[]> kafka, FileStorage storage,
                          @Value("${app.topic}") String topic) {
        this.kafka = kafka; this.storage = storage; this.topic = topic;
    }
    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, String>> upload(@RequestParam("file") MultipartFile file) throws Exception {
        String name = file.getOriginalFilename();
        validateName(name);
        if (file.getSize() > 900 * 1024) throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE);
        kafka.send(topic, name, file.getBytes()).get(35, TimeUnit.SECONDS);
        return ResponseEntity.accepted().body(Map.of("name", name, "status", "queued"));
    }
    @GetMapping public List<String> list() { return storage.list(); }
    @GetMapping("/{name}") public ResponseEntity<byte[]> download(@PathVariable String name,
            @RequestParam(defaultValue = "true") boolean download) {
        validateName(name);
        return ResponseEntity.ok().contentType(download ? MediaType.TEXT_PLAIN
                : new MediaType("text", "plain", StandardCharsets.UTF_8))
            .header("X-Content-Type-Options", "nosniff")
            .header(HttpHeaders.CONTENT_DISPOSITION, (download ? ContentDisposition.attachment() : ContentDisposition.inline())
                .filename(name, StandardCharsets.UTF_8).build().toString())
            .body(storage.read(name));
    }
    static void validateName(String name) {
        if (name == null || !name.toLowerCase(java.util.Locale.ROOT).endsWith(".txt") || name.length() > 200
            || name.contains("/") || name.contains("\\") || name.codePoints().anyMatch(Character::isISOControl)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Use a plain filename ending in .txt");
        }
    }
}
