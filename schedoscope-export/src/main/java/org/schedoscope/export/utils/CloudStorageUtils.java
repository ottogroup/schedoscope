package org.schedoscope.export.utils;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class CloudStorageUtils {

    final static private Random rnd = new Random();

    static public Storage storageService() {
        return StorageOptions.getDefaultInstance().getService();
    }


    static public Storage storageService(String gcpKey) throws IOException {
        if (gcpKey == null)
            return storageService();

        GoogleCredentials credentials = GoogleCredentials
                .fromStream(
                        new ByteArrayInputStream(Charset.forName("UTF-8").encode(gcpKey).array())
                );

        return StorageOptions.newBuilder().setCredentials(credentials).build().getService();
    }

    static public boolean existsBucket(Storage storageService, String bucket) {
        return storageService.get(bucket) != null;
    }

    static public Bucket createBucket(Storage storageService, String bucket, String region) {
        if (!existsBucket(storageService, bucket))
            return storageService.create(BucketInfo.newBuilder(bucket).setLocation(region != null ? region : "europe-west3").build());
        else
            return storageService.get(bucket);
    }

    static public void deleteBucket(Storage storageService, String bucket) {
        deleteBlob(storageService, bucket, "");
        storageService.delete(bucket);
    }

    static public void deleteBlob(Storage storageService, String bucket, String blobNameOrPrefix) {
        if (!existsBucket(storageService, bucket))
            return;

        Page<Blob> blobsToDelete = storageService.list(bucket, Storage.BlobListOption.prefix(blobNameOrPrefix));

        for (Blob blob : blobsToDelete.iterateAll()) {
            storageService.delete(blob.getBlobId());
        }
    }

    static public List<String> listBlobs(Storage storageService, String bucket, String blobNameOrPrefix) {
        List<String> result = new LinkedList<>();

        Page<Blob> blobs = storageService.list(bucket, Storage.BlobListOption.prefix(blobNameOrPrefix));

        for (Blob blob : blobs.iterateAll()) {
            result.add("gs://" + blob.getBucket() + "/" + blob.getName());
        }

        return result;
    }

    static public Blob createBlobIfNotExists(Storage storageService, BlobId blob, String region) {
        if (!existsBucket(storageService, blob.getBucket()))
            createBucket(storageService, blob.getBucket(), region);

        return storageService.create(BlobInfo.newBuilder(blob).setContentType("application/json").build());

    }

    static public Blob createBlobIfNotExists(Storage storageService, String bucket, String blobName, String region) {
        return createBlobIfNotExists(storageService, BlobId.of(bucket, blobName), region);
    }
}
