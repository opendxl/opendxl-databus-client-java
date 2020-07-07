package com.opendxl.databus.entities;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import io.findify.s3mock.S3Mock;
import junit.extensions.PA;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class S3TierStorageTestForUnreachableService {

    private static final String AWS_SECRET_KEY = "secretKey";
    private static final String AWS_ACCESS_KEY = "accessKey";
    private static final String AWS_REGION = "us-east-1";
    private static S3Mock api;
    private static AmazonS3Client client;


    @BeforeClass
    public static void beforeClass() {
        api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
        AwsClientBuilder.EndpointConfiguration endpoint =
                new AwsClientBuilder
                        .EndpointConfiguration("http://localhost:8001", "us-east-1");

        client = (AmazonS3Client) AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(endpoint)
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .build();
    }


    @Test
    public void shouldThrowAnExceptionWhenPutAnObject() {
        final String bucketName = "bucket-name";
        final String objectName = "object-name";
        final String objectRaw = "Hello!";
        final byte[] objectContent = objectRaw.getBytes();

        try {
            TierStorage tierStorage = new S3TierStorage(AWS_REGION, new ClientConfiguration());
            PA.setValue(tierStorage, "s3Client", client);
            tierStorage.put(bucketName, objectName, objectContent);
            Assert.fail("An Exception is expected");
        } catch (DatabusClientRuntimeException e) {
            Assert.assertTrue(true);
        } catch (Exception e) {
            Assert.fail("Unexpected Exception");
        }
    }

    @Test
    public void shouldThrowAnExceptionWhenGetAnObject() {
        final String bucketName = "bucket-name";
        final String objectName = "object-name";
        final String objectRaw = "Hello!";
        final byte[] objectContent = objectRaw.getBytes();

        try {
            TierStorage tierStorage = new S3TierStorage(AWS_REGION, new ClientConfiguration());
            PA.setValue(tierStorage, "s3Client", client);
            tierStorage.get(bucketName, objectName);
            Assert.fail("An Exception is expected");
        } catch (DatabusClientRuntimeException e) {
            Assert.assertTrue(true);
        } catch (Exception e) {
            Assert.fail("Unexpected Exception");
        }
    }

    @Test
    public void shouldThrowAnExceptionSearchAnObject() {
        final String bucketName = "bucket-name";
        final String objectName = "object-name";
        final String objectRaw = "Hello!";
        final byte[] objectContent = objectRaw.getBytes();

        try {
            TierStorage tierStorage = new S3TierStorage(AWS_REGION, new ClientConfiguration());
            PA.setValue(tierStorage, "s3Client", client);
            tierStorage.doesObjectExist(bucketName, objectName);
            Assert.fail("An Exception is expected");
        } catch (DatabusClientRuntimeException e) {
            Assert.assertTrue(true);
        } catch (Exception e) {
            Assert.fail("Unexpected Exception");
        }
    }

}