/**
 * Copyright 2015 Otto (GmbH & Co KG)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.export.utils;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

public class CloudStorageUtils {

    /**
     * Return an instance of the Google Cloud Storage web service authenticated using the GCP standard authentication
     * mechanism.
     *
     * @return the instance
     */
    static public Storage storageService() {
        return StorageOptions.getDefaultInstance().getService();
    }

    /**
     * Return an instance of the Google Cloud Storage web service.
     *
     * @return the instance
     */

    /**
     * Return an instance of the Google Cloud Storage web service authenticated using the given key.
     *
     * @param gcpKey the JSON formatted GCP key.
     * @return the instance
     * @throws IOException if a problem occurs parsing the key.
     */
    static public Storage storageService(String gcpKey) throws IOException {
        if (gcpKey == null)
            return storageService();

        GoogleCredentials credentials = GoogleCredentials
                .fromStream(
                        new ByteArrayInputStream(Charset.forName("UTF-8").encode(gcpKey).array())
                );

        return StorageOptions.newBuilder().setCredentials(credentials).build().getService();
    }

    /**
     * Check whether a bucket exists.
     *
     * @param storageService the storage service instance to use
     * @param bucket         the name of the bucket to check.
     * @return true iff the bucket exists.
     */
    static public boolean existsBucket(Storage storageService, String bucket) {
        return storageService.get(bucket) != null;
    }

    /**
     * Create a bucket if it does not exist already.
     *
     * @param storageService the storage service instance to use
     * @param bucket         the name of the bucket to create
     * @param region         the region to create the bucket in, europe-west3 if null
     * @return the bucket created or the bucket that already existed
     */
    static public Bucket createBucket(Storage storageService, String bucket, String region) {
        if (!existsBucket(storageService, bucket))
            return storageService.create(BucketInfo.newBuilder(bucket).setLocation(region != null ? region : "europe-west3").build());
        else
            return storageService.get(bucket);
    }

    /**
     * Delete a bucket including all the blobs within.
     *
     * @param storageService the storage service instance to use
     * @param bucket         the name of the bucket to delete
     */
    static public void deleteBucket(Storage storageService, String bucket) {
        deleteBlob(storageService, bucket, "");
        storageService.delete(bucket);
    }

    /**
     * Delete blobs in a bucket
     *
     * @param storageService   the storage service instance to use
     * @param bucket           the name of the bucket in which to delete blobs
     * @param blobNameOrPrefix the blob name prefix of the blobs to delete
     */
    static public void deleteBlob(Storage storageService, String bucket, String blobNameOrPrefix) {
        if (!existsBucket(storageService, bucket))
            return;

        Page<Blob> blobsToDelete = storageService.list(bucket, Storage.BlobListOption.prefix(blobNameOrPrefix));

        for (Blob blob : blobsToDelete.iterateAll()) {
            storageService.delete(blob.getBlobId());
        }
    }

    /**
     * List blobs matching a blob name prefix.
     *
     * @param storageService   the storage service instance to use
     * @param bucket           the name of the bucket in which to list blobs
     * @param blobNameOrPrefix the blob name prefix of the blobs to delete
     * @return the list of matching blob names.
     */
    static public List<String> listBlobs(Storage storageService, String bucket, String blobNameOrPrefix) {
        List<String> result = new LinkedList<>();

        Page<Blob> blobs = storageService.list(bucket, Storage.BlobListOption.prefix(blobNameOrPrefix));

        for (Blob blob : blobs.iterateAll()) {
            result.add("gs://" + blob.getBucket() + "/" + blob.getName());
        }

        return result;
    }

    /**
     * Create a blob in a bucket if it does not exist. If the bucket does not exist, it will be created.
     *
     * @param storageService the storage service instance to use
     * @param blob           the ID of the blob to create
     * @param region         the region where to create the bucket if it does not exist
     * @return the blob created or the blob that already existed.
     */
    static public Blob createBlobIfNotExists(Storage storageService, BlobId blob, String region) {
        if (!existsBucket(storageService, blob.getBucket()))
            createBucket(storageService, blob.getBucket(), region);

        return storageService.create(BlobInfo.newBuilder(blob).setContentType("application/json").build());

    }

    /**
     * Create a blob in a bucket if it does not exist. If the bucket does not exist, it will be created.
     *
     * @param storageService the storage service instance to use
     * @param bucket         the name of the bucket to create the blob in.
     * @param blobName       the name of the blob to create
     * @param region         the region where to create the bucket if it does not exist
     * @return the blob created or the blob that already existed.
     */
    static public Blob createBlobIfNotExists(Storage storageService, String bucket, String blobName, String region) {
        return createBlobIfNotExists(storageService, BlobId.of(bucket, blobName), region);
    }
}
