package com.opendxl.databus.entities;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class S3TierStorage implements TierStorage {

    /**
     * The logger object.
     */
    private static final Logger LOG = LoggerFactory.getLogger(S3TierStorage.class);

    /**
     * S3 client
     */
    private AmazonS3 s3Client;

    public S3TierStorage(final String awsRegion,
                         final ClientConfiguration config) {

        AmazonS3ClientBuilder s3Builder = AmazonS3ClientBuilder.standard();
        s3Builder.withCredentials(new InstanceProfileCredentialsProvider(false));
        s3Builder.withRegion(awsRegion);
        if (config != null) {
            s3Builder.withClientConfiguration(config);
        }
        try {
            this.s3Client = s3Builder.build();
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    public S3TierStorage(final String awsAccessKey,
                         final String awsSecretKey,
                         final String awsRegion,
                         final ClientConfiguration config) {


        AmazonS3ClientBuilder s3Builder = AmazonS3ClientBuilder.standard();
        s3Builder.withCredentials(
                new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(awsAccessKey, awsSecretKey)));
        s3Builder.withRegion(awsRegion);
        if (config != null) {
            s3Builder.withClientConfiguration(config);
        }

        try {
            this.s3Client = s3Builder.build();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Override
    public void put(final String s3BucketName,
                        final String s3KeyName,
                        final byte[] payload) {

        try {
            if (!s3Client.doesBucketExistV2(s3BucketName)) {
                s3Client.createBucket(s3BucketName);
            }

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(payload.length);
            metadata.setContentType(Mimetypes.MIMETYPE_HTML);
            InputStream s3Object = new ByteArrayInputStream(payload);
            PutObjectResult putObjectResult = s3Client.putObject(s3BucketName, s3KeyName, s3Object, metadata);

        } catch (Exception e) {
            final String errMsg = "Error uploading S3 object: Bucket: " + " Object: "
                    + s3KeyName + " " + e.getMessage();
            LOG.error(errMsg, e);
            throw new DatabusClientRuntimeException(errMsg, e, this.getClass());
        }

    }


    public boolean doesObjectExist(String s3BucketName, String s3KeyName) {
        try {
            return s3Client.doesObjectExist(s3BucketName, s3KeyName);
        } catch (Exception e) {
            final String errMsg = "Error trying to reach S3 object: Bucket: " + " Object: " + s3KeyName + " "
                    + e.getMessage();
            LOG.error(errMsg, e);
            throw new DatabusClientRuntimeException(errMsg, e, this.getClass());
        }
    }

    @Override
    public byte[] get(String s3BucketName, String s3KeyName) {
        try {
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(s3BucketName, s3KeyName));
            return IOUtils.toByteArray(s3Object.getObjectContent());
        } catch (Exception e) {
            final String errMsg = "Error reading S3 object: Bucket: " + " Object: " + s3KeyName + " " + e.getMessage();
            LOG.error(errMsg, e);
            throw new DatabusClientRuntimeException(errMsg, e, this.getClass());
        }
    }

}
