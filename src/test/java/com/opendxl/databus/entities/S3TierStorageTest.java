package com.opendxl.databus.entities;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import junit.extensions.PA;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

public class S3TierStorageTest {

    private static final String AWS_SECRET_KEY = "secretKey";
    private static final String AWS_ACCESS_KEY = "accessKey";
    private static final String AWS_REGION = "us-east-1";
    private static S3Mock api;
    private static AmazonS3Client client;


    @BeforeClass
    public static void beforeClass() {
        api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
        api.start();
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

    @AfterClass
    public static void afterClass() {
        api.shutdown(); // kills the underlying actor system. Use api.stop() to just unbind the port.
    }

    @Test
    public void shouldPutAngGetAnS3ObjectWithCredentials() {

        final String bucketName = "bucket-name";
        final String objectName = "object-name";
        final String objectRaw = "Hello!";
        final byte[] objectContent = objectRaw.getBytes();

        try {
            TierStorage tierStorage = new S3TierStorage(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION,
                    new ClientConfiguration());

            PA.setValue(tierStorage, "s3Client", client);
            tierStorage.put(bucketName, objectName, objectContent);

            Assert.assertTrue(tierStorage.doesObjectExist(bucketName, objectName));


            byte[] actualObjectContent = tierStorage.get(bucketName, objectName);
            Assert.assertTrue(Arrays.equals(actualObjectContent, objectContent));
            String actualObjectRaw = new String(actualObjectContent);
            Assert.assertTrue(actualObjectRaw.equals(objectRaw));

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void shouldPutAngGetAnS3ObjectWithoutCredentials() {

        final String bucketName = "bucket-name";
        final String objectName = "object-name";
        final String objectRaw = "Hello!";
        final byte[] objectContent = objectRaw.getBytes();

        try {
            TierStorage tierStorage = new S3TierStorage(AWS_REGION, new ClientConfiguration());

            PA.setValue(tierStorage, "s3Client", client);
            tierStorage.put(bucketName, objectName, objectContent);

            Assert.assertTrue(tierStorage.doesObjectExist(bucketName, objectName));

            byte[] actualObjectContent = tierStorage.get(bucketName, objectName);
            Assert.assertTrue(Arrays.equals(actualObjectContent, objectContent));
            String actualObjectRaw = new String(actualObjectContent);
            Assert.assertTrue(actualObjectRaw.equals(objectRaw));

        } catch (Exception e) {
            Assert.fail();
        }
    }

}