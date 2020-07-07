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
import com.amazonaws.services.s3.model.S3Object;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * It is a built-in AWS S3 Tier Storage.
 *
 * It implements mechanisms to upload and download AWS S3 objects
 */
public class S3TierStorage implements TierStorage {

    /**
     * The logger object.
     */
    private static final Logger LOG = LoggerFactory.getLogger(S3TierStorage.class);

    /**
     * S3 client
     */
    private AmazonS3 s3Client;

    /**
     * Constructor used to create a role-based authenticated tier storage instance.
     *
     * @param awsRegion AWS region
     * @param config AWS client configuration
     * @throws DatabusClientRuntimeException exception if the underlying AWS S3 client cannot be created
     */
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
            final String errMsg = "Error creating a S3 Tier Storage. Region: " + awsRegion + " " + e.getMessage();
            LOG.error(errMsg, e);
            throw new DatabusClientRuntimeException(errMsg, e, this.getClass());
        }
    }


    /**
     * Constructor used to create a tier storage instance with AWS access and secret key
     *
     * @param awsRegion AWS region
     * @param config AWS client configuration
     * @param awsAccessKey AWS access key
     * @param awsSecretKey AWS secret key
     * @throws DatabusClientRuntimeException exception if the underlying AWS S3 client cannot be created
     */
    public S3TierStorage(final String awsRegion,
                         final ClientConfiguration config,
                         final String awsAccessKey,
                         final String awsSecretKey) {


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
            final String errMsg = "Error creating a S3 Tier Storage. Region: " + awsRegion + " " + e.getMessage();
            LOG.error(errMsg, e);
            throw new DatabusClientRuntimeException(errMsg, e, this.getClass());
        }
    }


    /**
     * Upload a object to AWS S3 bucket
     *
     * @param s3BucketName AWS S3 bucket
     * @param s3KeyName AWS S3 object name
     * @param payload AWS object content
     * @throws DatabusClientRuntimeException exception if the underlying AWS S3 fails.
     *
     */
    @Override
    public void put(final String s3BucketName, final String s3KeyName, final byte[] payload) {

        try {
            if (!s3Client.doesBucketExistV2(s3BucketName)) {
                s3Client.createBucket(s3BucketName);
            }

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(payload.length);
            metadata.setContentType(Mimetypes.MIMETYPE_HTML);
            InputStream s3Object = new ByteArrayInputStream(payload);
            s3Client.putObject(s3BucketName, s3KeyName, s3Object, metadata);

        } catch (Exception e) {
            final String errMsg = "Error uploading S3 object: Bucket: " + " Object: "
                    + s3KeyName + " " + e.getMessage();
            LOG.error(errMsg, e);
            throw new DatabusClientRuntimeException(errMsg, e, this.getClass());
        }

    }


    /**
     * Check if an AWS S3 object exists
     *
     * @param s3BucketName AWS S3 bucket
     * @param s3KeyName AWS object name
     * @return a boolean
     * @throws DatabusClientRuntimeException exception if the underlying AWS S3 fails.
     */
    @Override
    public boolean doesObjectExist(final String s3BucketName, final String s3KeyName) {
        try {
            return s3Client.doesObjectExist(s3BucketName, s3KeyName);
        } catch (Exception e) {
            final String errMsg = "Error trying to find a S3 object: Bucket: " + " Object: " + s3KeyName + " "
                    + e.getMessage();
            LOG.error(errMsg, e);
            throw new DatabusClientRuntimeException(errMsg, e, this.getClass());
        }
    }


    /**
     * Download a AWS S3 object content
     *
     * @param s3BucketName AWS S3 bucket name
     * @param s3KeyName AWS S3 object name
     * @return the object content
     * @throws DatabusClientRuntimeException exception if the underlying AWS S3 fails.
     */
    @Override
    public byte[] get(final String s3BucketName, final String s3KeyName) {
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
