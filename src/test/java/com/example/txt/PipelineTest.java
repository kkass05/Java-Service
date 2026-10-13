package com.example.txt;

import org.junit.jupiter.api.Test;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.server.ResponseStatusException;
import java.util.concurrent.CompletableFuture;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PipelineTest {
    @Test void preservesBytesFromUploadThroughConsumerToDownload() throws Exception {
        // UTF-8 BOM, CRLF, non-ASCII bytes, and a trailing newline.
        byte[] bytes = {(byte)0xef, (byte)0xbb, (byte)0xbf, (byte)0xd0, (byte)0x90, 13, 10};
        @SuppressWarnings("unchecked") KafkaTemplate<String, byte[]> kafka = mock(KafkaTemplate.class);
        FileStorage storage = mock(FileStorage.class);
        when(kafka.send(eq("txt-files"), eq("hello.txt"), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        FileController controller = new FileController(kafka, storage, "txt-files");
        assertEquals(202, controller.upload(new MockMultipartFile("file", "hello.txt", "text/plain", bytes))
            .getStatusCode().value());
        var captured = org.mockito.ArgumentCaptor.forClass(byte[].class);
        verify(kafka).send(eq("txt-files"), eq("hello.txt"), captured.capture());
        assertArrayEquals(bytes, captured.getValue());
        new FileConsumer(storage).receive(new ConsumerRecord<>("txt-files", 0, 0, "hello.txt", captured.getValue()));
        verify(storage).save(eq("hello.txt"), captured.capture());
        assertArrayEquals(bytes, captured.getValue());
        when(storage.read("hello.txt")).thenReturn(captured.getValue());
        assertArrayEquals(bytes, controller.download("hello.txt", true).getBody());
    }
    @Test void rejectsUnsafeNames() {
        for (String name : new String[]{"../a.txt", "C:\\a.txt", "a.txt\r\n", "image.png"}) {
            assertThrows(ResponseStatusException.class, () -> FileController.validateName(name));
        }
    }
}
