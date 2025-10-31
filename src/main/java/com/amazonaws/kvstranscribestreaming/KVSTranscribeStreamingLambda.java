package com.amazonaws.kvstranscribestreaming;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import com.amazonaws.transcribestreaming.TranscribeStreamingRetryClient;
import com.salesforce.scv.SCVLoggingUtil;

import java.util.Map;
import java.util.Optional;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.transcribestreaming.model.AudioEvent;
import software.amazon.awssdk.services.transcribestreaming.model.AudioStream;
import software.amazon.awssdk.services.transcribestreaming.model.LanguageCode;
import software.amazon.awssdk.services.transcribestreaming.model.Specialty;
import software.amazon.awssdk.services.transcribestreaming.model.VocabularyFilterMethod;
import software.amazon.awssdk.services.transcribestreaming.model.MediaEncoding;
import software.amazon.awssdk.services.transcribestreaming.model.TranscribeStreamingRequest;
import software.amazon.awssdk.services.transcribestreaming.model.StartStreamTranscriptionRequest;
import software.amazon.awssdk.services.transcribestreaming.model.StartMedicalStreamTranscriptionRequest;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * KVS Transcriber Lambda Handler with DynamoDB support
 * This version saves transcripts to DynamoDB tables
 */
public class KVSTranscribeStreamingLambda implements RequestHandler<Map<String, Object>, String> {

    private static final String REGION = System.getenv("APP_REGION");
    private static final String START_SELECTOR_TYPE = System.getenv("START_SELECTOR_TYPE");
    public static final MetricsUtil metricsUtil = new MetricsUtil(CloudWatchClient.create());
    private TranscribedSegmentWriter fromCustomerSegmentWriter = null;
    private TranscribedSegmentWriter toCustomerSegmentWriter = null;

    /**
     * Get TRANSCRIBE_REGION from configuration object
     */
    private static String getTranscribeRegion(ConfigManager.SecretConfig config) {
        String transcribeRegion = config.getConfigValue("TRANSCRIBE_REGION");
        return transcribeRegion != null ? transcribeRegion : "us-east-1";
    }

    /**
     * Handler function for the Lambda
     */
    @Override
    public String handleRequest(Map<String, Object> input, Context context) {
        Map<String, String> loggingContext = new HashMap<>();
        String connectContactId = (String) input.get("connectContactId");
        loggingContext.put(SCVLoggingUtil.VOICECALL_CONTEXT_KEY.VOICE_CALL_ID.toString(), connectContactId);
        
        try {
            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.KVSTranscribeStreamingLambda.handleRequest", 
                    SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, "Start Handle Request", loggingContext);

            // Extract values from the actual input format
            String streamARN = (String) input.get("streamARN");
            String startFragmentNum = (String) input.get("startFragmentNum");
            String languageCode = (String) input.get("languageCode");
            Boolean streamAudioFromCustomer = (Boolean) input.get("streamAudioFromCustomer");
            Boolean streamAudioToCustomer = (Boolean) input.get("streamAudioToCustomer");
            
            // Extract instance ARN from stream ARN
            String instanceARN = extractInstanceARNFromStreamARN(streamARN);
            
            // Create TranscriptionRequest object
            TranscriptionRequest request = new TranscriptionRequest();
            request.setInstanceARN(instanceARN);
            request.setStreamARN(streamARN);
            request.setStartFragmentNum(startFragmentNum);
            request.setVoiceCallId(connectContactId);
            request.setCustomerPhoneNumber(""); // Not available in input
            request.setLanguageCode(languageCode);
            request.setAudioStartTimestamp(String.valueOf(System.currentTimeMillis()));
            request.setStreamAudioFromCustomer(streamAudioFromCustomer != null ? streamAudioFromCustomer : true);
            request.setStreamAudioToCustomer(streamAudioToCustomer != null ? streamAudioToCustomer : false);
            request.setEngine("standard"); // Default to standard
            request.setVocabularyName(null);
            request.setVocabularyFilterName(null);
            request.setVocabularyFilterMethod(null);
            request.setSpecialty(null);

            ConfigManager.SecretConfig config = ConfigManager.getSecretConfig(null);
            startRealTranscribeStreaming(request.getInstanceARN(), request.getStreamARN(), 
                    request.getStartFragmentNum(), request.getVoiceCallId(), request.getLanguageCode(),
                    request.getAudioStartTimestamp(), request.getCustomerPhoneNumber(), 
                    request.isStreamAudioFromCustomer(), request.isStreamAudioToCustomer(), 
                    request.getEngine(), request.getVocabularyName(), request.getVocabularyFilterName(), 
                    request.getVocabularyFilterMethod(), request.getSpecialty(), config);

            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.KVSTranscribeStreamingLambda.handleRequest", 
                    SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, "End Handle Request", loggingContext);
            return "{ \"result\": \"Success\" }";
        } catch (Exception e) {
            SCVLoggingUtil.error("com.amazonaws.kvstranscribestreaming.KVSTranscribeStreamingLambda.handleRequest", 
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, e.getMessage(), loggingContext);
            return "{ \"result\": \"Failed\" }";
        }
    }

    /**
     * Starts streaming between KVS and Transcribe
     * At end of the streaming session, the raw audio is saved as an s3 object
     */
    private void startRealTranscribeStreaming(String instanceARN, String streamARN, String startFragmentNum, String voiceCallId, Optional<String> languageCode,
                                                   long audioStartTimestamp, String customerPhoneNumber, boolean isStreamAudioFromCustomerEnabled, boolean isStreamAudioToCustomerEnabled, String engine,
                                                   Optional<String> vocabularyName, Optional<String> vocabularyFilterName, Optional<String> vocabularyFilterMethod, Optional<String> specialty, ConfigManager.SecretConfig config) throws Exception {

            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.KVSTranscribeStreamingLambda.startRealTranscribeStreaming", SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, "START KVS Transcribe Streaming", null);

            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.KVSTranscribeStreamingLambda.startRealTranscribeStreaming",
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                    "Using configuration from environment variables for voiceCallId: " + voiceCallId,
                    null);

            // Initialize DynamoDB client
            DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
                    .region(Region.of(REGION))
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();

            // Initialize individual writers
            DynamoDBTranscribedSegmentWriter fromCustomerDynamoWriter = new DynamoDBTranscribedSegmentWriter(instanceARN, voiceCallId, true, audioStartTimestamp, customerPhoneNumber, config, dynamoDbClient);
            DynamoDBTranscribedSegmentWriter toCustomerDynamoWriter = new DynamoDBTranscribedSegmentWriter(instanceARN, voiceCallId, false, audioStartTimestamp, customerPhoneNumber, config, dynamoDbClient);
            
            WebSocketTranscribedSegmentWriter fromCustomerWebSocketWriter = new WebSocketTranscribedSegmentWriter(instanceARN, voiceCallId, true, audioStartTimestamp, customerPhoneNumber, config);
            WebSocketTranscribedSegmentWriter toCustomerWebSocketWriter = new WebSocketTranscribedSegmentWriter(instanceARN, voiceCallId, false, audioStartTimestamp, customerPhoneNumber, config);

            // Create composite writers that send to both DynamoDB and WebSocket
            fromCustomerSegmentWriter = new CompositeTranscribedSegmentWriter(fromCustomerDynamoWriter, fromCustomerWebSocketWriter);
            toCustomerSegmentWriter = new CompositeTranscribedSegmentWriter(toCustomerDynamoWriter, toCustomerWebSocketWriter);

            String streamName = streamARN.substring(streamARN.indexOf("/") + 1, streamARN.lastIndexOf("/"));
            String transcribeRegion = getTranscribeRegion(config);
            String transcribeEndpoint = "https://transcribestreaming." + transcribeRegion + ".amazonaws.com";

            // Get actual KVS input streams
            InputStream fromCustomerStream = null;
            InputStream toCustomerStream = null;

            if (isStreamAudioFromCustomerEnabled) {
                fromCustomerStream = RealKVSUtils.getInputStreamFromKVS(streamName, REGION, startFragmentNum, getTranscribeCredentials(), START_SELECTOR_TYPE);
            }
            if (isStreamAudioToCustomerEnabled) {
                toCustomerStream = RealKVSUtils.getInputStreamFromKVS(streamName, REGION, startFragmentNum, getTranscribeCredentials(), START_SELECTOR_TYPE);
            }

            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.KVSTranscribeStreamingLambda.startRealTranscribeStreaming",
                    SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, "START Initialize Transcribe client ", null);

            try (TranscribeStreamingRetryClient client = new TranscribeStreamingRetryClient(getTranscribeCredentials(), transcribeEndpoint,
                    Region.of(transcribeRegion), metricsUtil)) {

                SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.KVSTranscribeStreamingLambda.startRealTranscribeStreaming",
                        SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, "END Initialize Transcribe client ", null);

                CompletableFuture<Void> fromCustomerResult = null;
                CompletableFuture<Void> toCustomerResult = null;

                if (fromCustomerStream != null) {
                    SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.KVSTranscribeStreamingLambda.getStartStreamingTranscriptionFuture", SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, "START Get Transcribing Future for FROM_CUSTOMER stream", null);
                    fromCustomerResult = getStartStreamingTranscriptionFuture(fromCustomerStream,
                            languageCode, voiceCallId, client, fromCustomerSegmentWriter, "FROM_CUSTOMER", engine, vocabularyName, vocabularyFilterName, vocabularyFilterMethod, specialty);
                }

                if (toCustomerStream != null) {
                    SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.KVSTranscribeStreamingLambda.getStartStreamingTranscriptionFuture", SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, "START Get Transcribing Future for TO_CUSTOMER stream", null);
                    toCustomerResult = getStartStreamingTranscriptionFuture(toCustomerStream,
                            languageCode, voiceCallId, client, toCustomerSegmentWriter, "TO_CUSTOMER", engine, vocabularyName, vocabularyFilterName, vocabularyFilterMethod, specialty);
                }

                // Synchronous wait for stream to close, and close client connection
                // Timeout of 890 seconds because the Lambda function can be run for at most 15 mins (~890 secs)
                if (null != fromCustomerResult) {
                    fromCustomerResult.get(890, TimeUnit.SECONDS);
                }

                if (null != toCustomerResult) {
                    toCustomerResult.get(890, TimeUnit.SECONDS);
                }
            } catch (TimeoutException e) {
                SCVLoggingUtil.error("com.amazonaws.kvstranscribestreaming.KVSTranscribeStreamingLambda.startRealTranscribeStreaming", SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, e.getMessage(), null);
            } catch (Exception e) {
                SCVLoggingUtil.error("com.amazonaws.kvstranscribestreaming.KVSTranscribeStreamingLambda.startRealTranscribeStreaming", SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, e.getMessage(), null);
                throw e;
            } finally {
                // Clean up DynamoDB writers
                if (fromCustomerSegmentWriter != null) {
                    fromCustomerSegmentWriter.close();
                }
                if (toCustomerSegmentWriter != null) {
                    toCustomerSegmentWriter.close();
                }
            }
            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.KVSTranscribeStreamingLambda.startRealTranscribeStreaming", SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, "END KVS Transcribe Streaming", null);
        }

        private CompletableFuture<Void> getStartStreamingTranscriptionFuture(InputStream kvsInputStream, Optional<String> languageCodeOptional, String contactId, TranscribeStreamingRetryClient client,
                                                                             TranscribedSegmentWriter transcribedSegmentWriter, String channel, String engine, Optional<String> vocabularyName,
                                                                             Optional<String> vocabularyFilterName, Optional<String> vocabularyFilterMethod, Optional<String> specialty) {
            String languageCode = languageCodeOptional.isPresent() ? languageCodeOptional.get() : LanguageCode.EN_US.toString();
            TranscribeStreamingRequest request;
            if (engine.equals("medical")) {
                request = getMedicalRequest(languageCode, specialty, vocabularyName);
            } else {
                request = getStandardRequest(languageCode, vocabularyName, vocabularyFilterName, vocabularyFilterMethod);
            }

            return client.startStreamTranscription(
                    request,
                    new KVSAudioStreamPublisher(kvsInputStream),
                    new StreamTranscriptionBehaviorImpl(transcribedSegmentWriter),
                    channel,
                    engine
            );
        }

        /**
         * Build StartStreamTranscriptionRequest containing required parameters to open a streaming transcription request
         */
        private static StartStreamTranscriptionRequest getStandardRequest(String languageCode, Optional<String> vocabularyName, Optional<String> vocabularyFilterName, Optional<String> vocabularyFilterMethod) {
            StartStreamTranscriptionRequest.Builder builder = StartStreamTranscriptionRequest.builder();
            builder.languageCode(languageCode);
            builder.mediaEncoding(MediaEncoding.PCM);
            builder.mediaSampleRateHertz(8000);
            builder.sessionId(UUID.randomUUID().toString());
            if (vocabularyName.isPresent()) {
                builder.vocabularyName(vocabularyName.get());
            }
            if (vocabularyFilterName.isPresent()) {
                builder.vocabularyFilterName(vocabularyFilterName.get());
                builder.vocabularyFilterMethod(VocabularyFilterMethod.fromValue(vocabularyFilterMethod.isPresent() ? vocabularyFilterMethod.get() : VocabularyFilterMethod.MASK.toString()));
            }
            return builder.build();
        }

        /**
         * Build StartMedicalStreamTranscriptionRequest containing required parameters to open a streaming transcription request
         */
        private static StartMedicalStreamTranscriptionRequest getMedicalRequest(String languageCode, Optional<String> specialty, Optional<String> vocabularyName) {
            StartMedicalStreamTranscriptionRequest.Builder builder = StartMedicalStreamTranscriptionRequest.builder();
            builder.languageCode(languageCode);
            builder.mediaEncoding(MediaEncoding.PCM);
            builder.mediaSampleRateHertz(8000);
            builder.specialty(Specialty.fromValue(String.valueOf(specialty.isPresent() ? specialty.get() : Specialty.PRIMARYCARE.toString())));
            builder.type("CONVERSATION");
            builder.sessionId(UUID.randomUUID().toString());
            if (vocabularyName.isPresent()){
                builder.vocabularyName(vocabularyName.get());
            }
            return builder.build();
        }

        /**
         * KVSAudioStreamPublisher implements audio stream publisher.
         * It emits audio events from a KVS stream asynchronously in a separate thread
         */
        private static class KVSAudioStreamPublisher implements Publisher<AudioStream> {
            private final InputStream kvsInputStream;
            private ExecutorService executor = Executors.newFixedThreadPool(1);
            private AtomicLong demand = new AtomicLong(0);

            private KVSAudioStreamPublisher(InputStream kvsInputStream) {
                this.kvsInputStream = kvsInputStream;
            }

            @Override
            public void subscribe(Subscriber<? super AudioStream> s) {
                s.onSubscribe(new KVSByteToAudioEventSubscription(s, kvsInputStream));
            }
        }

        /**
         * This Subscription converts audio bytes received from the KVS stream into AudioEvents
         * that can be sent to the Transcribe service. It implements a simple demand system that will read chunks of bytes
         * from a KVS stream.
         */
        private static class KVSByteToAudioEventSubscription implements Subscription {

            private static final Logger logger = LoggerFactory.getLogger(KVSByteToAudioEventSubscription.class);
            private static final int CHUNK_SIZE_IN_BYTES = 8192; // 8KB for better transcription quality
            private ExecutorService executor = Executors.newFixedThreadPool(1);
            private AtomicLong demand = new AtomicLong(0);
            private final Subscriber<? super AudioStream> subscriber;
            private final InputStream kvsInputStream;

            public KVSByteToAudioEventSubscription(Subscriber<? super AudioStream> s, InputStream kvsInputStream) {
                this.subscriber = s;
                this.kvsInputStream = kvsInputStream;
            }

            @Override
            public void request(long n) {
                if (n <= 0) {
                    subscriber.onError(new IllegalArgumentException("Demand must be positive"));
                }

                demand.getAndAdd(n);
                executor.submit(() -> {
                    try {
                        while (demand.get() > 0) {
                            byte[] buffer = new byte[CHUNK_SIZE_IN_BYTES];
                            int bytesRead = kvsInputStream.read(buffer);

                            if (bytesRead > 0) {
                                // Validate audio chunk size and quality
                                if (bytesRead >= 1024) { // Minimum 1KB for quality audio
                                    ByteBuffer audioBuffer = ByteBuffer.wrap(buffer, 0, bytesRead);
                                    
                                    // Log audio processing for debugging
                                    logger.debug("Processing audio chunk: {} bytes", bytesRead);
                                    
                                    AudioEvent audioEvent = AudioEvent.builder()
                                            .audioChunk(software.amazon.awssdk.core.SdkBytes.fromByteBuffer(audioBuffer))
                                            .build();
                                    subscriber.onNext(audioEvent);
                                    
                                    // Add small delay to prevent overwhelming the transcription service
                                    Thread.sleep(10);
                                } else {
                                    logger.debug("Skipping small audio chunk: {} bytes", bytesRead);
                                }
                            } else if (bytesRead == -1) {
                                logger.info("End of audio stream reached");
                                subscriber.onComplete();
                                break;
                            }
                            demand.getAndDecrement();
                        }
                    } catch (IOException e) {
                        logger.error("Error reading from KVS input stream: {}", e.getMessage(), e);
                        subscriber.onError(e);
                    } catch (InterruptedException e) {
                        logger.warn("Audio processing interrupted: {}", e.getMessage());
                        Thread.currentThread().interrupt();
                    }
                });
            }

            @Override
            public void cancel() {
                executor.shutdown();
                try {
                    kvsInputStream.close();
                } catch (IOException e) {
                    logger.error("Error closing KVS input stream: {}", e.getMessage(), e);
                }
            }
        }

        /**
         * Extract instance ARN from stream ARN
         * Stream name format: live-audio-connect-travoiqbot-contact-{instanceId}
         */
        private String extractInstanceARNFromStreamARN(String streamARN) {
            try {
                // Extract stream name from ARN
                String streamName = streamARN.substring(streamARN.lastIndexOf("/") + 1);
                String[] parts = streamName.split("-");
                if (parts.length >= 5) {
                    String instanceId = parts[4]; // Assuming instance ID is the 5th part
                    return "arn:aws:connect:us-west-2:196031534589:instance/" + instanceId;
                }
                // Fallback: extract from the full stream name pattern
                if (streamName.startsWith("live-audio-connect-travoiqbot-contact-")) {
                    String instanceId = streamName.substring("live-audio-connect-travoiqbot-contact-".length());
                    return "arn:aws:connect:us-west-2:196031534589:instance/" + instanceId;
                }
                return "arn:aws:connect:us-west-2:196031534589:instance/unknown";
            } catch (Exception e) {
                SCVLoggingUtil.error("com.amazonaws.kvstranscribestreaming.KVSTranscribeStreamingLambda.extractInstanceARNFromStreamARN", 
                        SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, 
                        "Error extracting instance ARN from stream ARN: " + e.getMessage(), null);
                return "arn:aws:connect:us-west-2:196031534589:instance/unknown";
            }
        }

        /**
         * @return AWS credentials to be used to connect to all AWS services
         */
        private static AwsCredentialsProvider getTranscribeCredentials() {
            return DefaultCredentialsProvider.create();
        }
    }
