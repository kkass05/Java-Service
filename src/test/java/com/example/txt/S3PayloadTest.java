package com.example.txt;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import static org.junit.jupiter.api.Assertions.*;

class S3PayloadTest {
    @Test void sendsRawBytesWithoutS3StreamingEnvelope() throws Exception {
        byte[] original = {(byte)0xef, (byte)0xbb, (byte)0xbf, (byte)0xd0, (byte)0x90, 13, 10};
        AtomicReference<byte[]> received = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            received.set(exchange.getRequestBody().readAllBytes());
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        try (var s3 = new PipelineConfig().s3("http://127.0.0.1:" + server.getAddress().getPort(), "demo", "demo-secret")) {
            s3.putObject(PutObjectRequest.builder().bucket("txt-files").key("hello.txt").build(),
                RequestBody.fromBytes(original));
            assertArrayEquals(original, received.get());
        } finally { server.stop(0); }
    }
}
