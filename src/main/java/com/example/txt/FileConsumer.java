package com.example.txt;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Profile("consumer")
public class FileConsumer {
    private final FileStorage storage;
    public FileConsumer(FileStorage storage) { this.storage = storage; }
    @KafkaListener(topics = "${app.topic}")
    public void receive(ConsumerRecord<String, byte[]> record) {
        storage.save(record.key(), record.value());
    }
}
