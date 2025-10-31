package com.amazonaws.kvstranscribestreaming;

import com.salesforce.scv.SCVLoggingUtil;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * DynamoDB implementation of TranscribedSegmentWriter
 * Saves transcripts to DynamoDB tables
 */
public class DynamoDBTranscribedSegmentWriter implements TranscribedSegmentWriter {

    private final String instanceARN;
    private final String voiceCallId;
    private final boolean isFromCustomer;
    private final long audioStartTimestamp;
    private final String customerPhoneNumber;
    private final ConfigManager.SecretConfig config;
    private final DynamoDbClient dynamoDbClient;
    private final String tableName;

    public DynamoDBTranscribedSegmentWriter(String instanceARN, String voiceCallId, boolean isFromCustomer, 
                                          long audioStartTimestamp, String customerPhoneNumber, 
                                          ConfigManager.SecretConfig config, DynamoDbClient dynamoDbClient) {
        this.instanceARN = instanceARN;
        this.voiceCallId = voiceCallId;
        this.isFromCustomer = isFromCustomer;
        this.audioStartTimestamp = audioStartTimestamp;
        this.customerPhoneNumber = customerPhoneNumber;
        this.config = config;
        this.dynamoDbClient = dynamoDbClient;
        
        // Set table name based on direction
        if (isFromCustomer) {
            this.tableName = "stage-travoiq-recording-contactTranscriptSegments";
        } else {
            this.tableName = "stage-travoiq-recording-contactTranscriptSegmentsToCustomer";
        }
    }

    @Override
    public void write(String transcript, String messageId, long startTime, long endTime, boolean isPartial) {
        try {
            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.DynamoDBTranscribedSegmentWriter.write", 
                    SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, 
                    "Saving transcript to DynamoDB table: " + tableName + " (partial: " + isPartial + ")", null);

            // Create DynamoDB item with only required columns
            Map<String, AttributeValue> item = new HashMap<>();
            
            // Primary key - ContactId is required by DynamoDB table schema
            item.put("ContactId", AttributeValue.builder().s(voiceCallId).build());
            item.put("segmentId", AttributeValue.builder().s(messageId).build());
            
            // Transcript data - StartTime is the sort key in DynamoDB table
            item.put("Transcript", AttributeValue.builder().s(transcript).build());
            item.put("StartTime", AttributeValue.builder().n(String.valueOf(startTime)).build());
            item.put("EndTime", AttributeValue.builder().n(String.valueOf(endTime)).build());
            item.put("IsPartial", AttributeValue.builder().bool(isPartial).build());
            
            // LoggedOn timestamp
            item.put("LoggedOn", AttributeValue.builder().n(String.valueOf(System.currentTimeMillis())).build());
            
            // Create PutItem request
            PutItemRequest putItemRequest = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build();

            // Save to DynamoDB
            PutItemResponse response = dynamoDbClient.putItem(putItemRequest);
            
            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.DynamoDBTranscribedSegmentWriter.write", 
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, 
                    "Transcript saved successfully to DynamoDB table: " + tableName + 
                    " (segmentId: " + messageId + ", partial: " + isPartial + ")", null);

        } catch (Exception e) {
            SCVLoggingUtil.error("com.amazonaws.kvstranscribestreaming.DynamoDBTranscribedSegmentWriter.write", 
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, 
                    "Failed to save transcript to DynamoDB: " + e.getMessage(), null);
        }
    }

    @Override
    public void close() {
        // DynamoDB client is managed externally, no cleanup needed
        SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.DynamoDBTranscribedSegmentWriter.close", 
                SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, 
                "DynamoDBTranscribedSegmentWriter closed for table: " + tableName, null);
    }
}
