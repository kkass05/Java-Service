package com.example.txt;

import java.net.URI;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
public class PipelineConfig {
    @Bean NewTopic filesTopic(@Value("${app.topic}") String topic) {
        return TopicBuilder.name(topic).partitions(1).replicas(1).build();
    }
    @Bean DefaultErrorHandler errorHandler() {
        // Keep retrying storage errors; never discard a file and commit its offset.
        return new DefaultErrorHandler(new FixedBackOff(2000, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
    @Bean S3Client s3(@Value("${app.s3-endpoint}") String endpoint,
                     @Value("${app.access-key}") String key, @Value("${app.secret-key}") String secret) {
        return S3Client.builder().endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1)
            // SeaweedFS 3.85 must receive the raw payload, without aws-chunked trailers.
            .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
            .serviceConfiguration(S3Configuration.builder().chunkedEncodingEnabled(false).build())
            .forcePathStyle(true).credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(key, secret))).build();
    }
}
