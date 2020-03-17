package com.opendxl.databus.entities;

public interface TierStorage {
    void put(String bucketName, String objectName, byte[] payload);
    byte[] get(String bucketName, String objectName);
    boolean doesObjectExist(String bucketName, String objectName);
}

