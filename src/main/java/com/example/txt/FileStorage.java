package com.example.txt;

import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

@Service
public class FileStorage {
    private final S3Client s3;
    private final String bucket;
    public FileStorage(S3Client s3, @Value("${app.bucket}") String bucket) {
        this.s3 = s3; this.bucket = bucket;
    }
    public void save(String name, byte[] bytes) {
        ensureBucket();
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(name)
            .contentType("text/plain").build(), RequestBody.fromBytes(bytes));
    }
    public byte[] read(String name) {
        return s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(name).build()).asByteArray();
    }
    public List<String> list() {
        ensureBucket();
        return s3.listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(bucket).build())
            .contents().stream().map(S3Object::key).sorted().toList();
    }
    private synchronized void ensureBucket() {
        try { s3.headBucket(HeadBucketRequest.builder().bucket(bucket).build()); }
        catch (S3Exception e) {
            if (e.statusCode() != 404) throw e;
            try { s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build()); }
            catch (S3Exception race) { if (race.statusCode() != 409) throw race; }
        }
    }
}
