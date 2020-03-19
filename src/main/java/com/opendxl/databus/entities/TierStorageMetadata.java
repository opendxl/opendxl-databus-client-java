package com.opendxl.databus.entities;

public class TierStorageMetadata {

    private final String bucketName;
    private final String objectName;

    public TierStorageMetadata(final String bucketName, final String objectName) {
        this.bucketName = bucketName.trim();
        this.objectName = objectName.trim();
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getObjectName() {
        return objectName;
    }


}
