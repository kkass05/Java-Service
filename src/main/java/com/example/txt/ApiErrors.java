package com.example.txt;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Exception;

@RestControllerAdvice
public class ApiErrors {
    @ExceptionHandler(S3Exception.class)
    ResponseEntity<Map<String, String>> storage(S3Exception e) {
        return ResponseEntity.status(e.statusCode() == 404 ? 404 : 503)
            .body(Map.of("error", e.statusCode() == 404 ? "File not found" : "Storage unavailable"));
    }
    @ExceptionHandler({ExecutionException.class, TimeoutException.class, SdkClientException.class})
    ResponseEntity<Map<String, String>> unavailable(Exception e) {
        return ResponseEntity.status(503).body(Map.of("error", "Service unavailable; upload outcome may be unknown"));
    }
    @ExceptionHandler(MaxUploadSizeExceededException.class)
    ResponseEntity<Map<String, String>> tooLarge(Exception e) {
        return ResponseEntity.status(413).body(Map.of("error", "Maximum file size: 900 KiB"));
    }
}
