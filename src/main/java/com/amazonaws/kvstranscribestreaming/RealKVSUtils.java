package com.amazonaws.kvstranscribestreaming;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesisvideo.KinesisVideoClient;
import software.amazon.awssdk.services.kinesisvideo.model.GetDataEndpointRequest;
import software.amazon.awssdk.services.kinesisvideomedia.KinesisVideoMediaClient;
import software.amazon.awssdk.services.kinesisvideomedia.model.GetMediaRequest;
import software.amazon.awssdk.services.kinesisvideomedia.model.StartSelector;
import software.amazon.awssdk.services.kinesisvideomedia.model.StartSelectorType;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Real KVS Utils that works with actual Kinesis Video Streams using AWS SDK v2
 * This implementation uses AWS SDK v2 for all KVS operations
 */
public final class RealKVSUtils {

    public enum TrackName {
        AUDIO_FROM_CUSTOMER("AUDIO_FROM_CUSTOMER"),
        AUDIO_TO_CUSTOMER("AUDIO_TO_CUSTOMER");

        private String name;

        TrackName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(RealKVSUtils.class);

    /**
     * Makes a GetMedia call to KVS and retrieves the InputStream corresponding to the given streamName and startFragmentNum
     */
    public static InputStream getInputStreamFromKVS(String streamName,
                                                    String region,
                                                    String startFragmentNum,
                                                    AwsCredentialsProvider awsCredentialsProvider,
                                                    String startSelectorType) {
        Validate.notNull(streamName);
        Validate.notNull(region);
        Validate.notNull(startFragmentNum);
        Validate.notNull(awsCredentialsProvider);

        try {
            // Create Kinesis Video client
            KinesisVideoClient kinesisVideoClient = KinesisVideoClient.builder()
                    .region(Region.of(region))
                    .credentialsProvider(awsCredentialsProvider)
                    .build();

            // Get data endpoint
            String dataEndpoint = kinesisVideoClient.getDataEndpoint(GetDataEndpointRequest.builder()
                    .apiName("GET_MEDIA")
                    .streamName(streamName)
                    .build()).dataEndpoint();

            logger.info("Data endpoint for stream {}: {}", streamName, dataEndpoint);

            // Create Kinesis Video Media client
            KinesisVideoMediaClient kinesisVideoMediaClient = KinesisVideoMediaClient.builder()
                    .region(Region.of(region))
                    .credentialsProvider(awsCredentialsProvider)
                    .endpointOverride(java.net.URI.create(dataEndpoint))
                    .build();

            // Create start selector
            StartSelector startSelector;
            if ("FRAGMENT_NUMBER".equals(startSelectorType)) {
                startSelector = StartSelector.builder()
                        .startSelectorType(StartSelectorType.FRAGMENT_NUMBER)
                        .afterFragmentNumber(startFragmentNum)
                        .build();
                logger.info("StartSelector set to FRAGMENT_NUMBER: {}", startFragmentNum);
            } else {
                startSelector = StartSelector.builder()
                        .startSelectorType(StartSelectorType.NOW)
                        .build();
                logger.info("StartSelector set to NOW");
            }

            // Get media stream
            GetMediaRequest getMediaRequest = GetMediaRequest.builder()
                    .streamName(streamName)
                    .startSelector(startSelector)
                    .build();

            var getMediaResponse = kinesisVideoMediaClient.getMedia(getMediaRequest);
            
            logger.info("GetMedia called on stream {} - response received", streamName);

            return getMediaResponse;

        } catch (Exception e) {
            logger.error("Error getting KVS stream for streamName: {}, region: {}, error: {}", streamName, region, e.getMessage(), e);
            throw new RuntimeException("Failed to get KVS stream for streamName: " + streamName + ", region: " + region + ", error: " + e.getMessage(), e);
        }
    }

    /**
     * Simple audio data extraction from KVS stream
     * This is a simplified version that reads raw audio data from the stream
     */
    public static ByteBuffer getByteBufferFromStream(InputStream kvsInputStream, String contactId, int chunkSizeInKB, String track) {
        try {
            byte[] buffer = new byte[chunkSizeInKB * 1024];
            int bytesRead = kvsInputStream.read(buffer);
            
            if (bytesRead > 0) {
                return ByteBuffer.wrap(buffer, 0, bytesRead);
            } else {
                return ByteBuffer.allocate(0);
            }
        } catch (Exception e) {
            logger.error("Error reading from KVS stream: {}", e.getMessage(), e);
            return ByteBuffer.allocate(0);
        }
    }

    /**
     * Simple audio data extraction from KVS stream (single chunk)
     */
    public static ByteBuffer getByteBufferFromStream(InputStream kvsInputStream, String contactId, String track) {
        return getByteBufferFromStream(kvsInputStream, contactId, 1, track);
    }
}