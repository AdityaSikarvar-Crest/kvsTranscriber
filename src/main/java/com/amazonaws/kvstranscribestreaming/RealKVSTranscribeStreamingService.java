package com.amazonaws.kvstranscribestreaming;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.transcribestreaming.StreamTranscriptionBehaviorImpl;
import com.amazonaws.transcribestreaming.TranscribeStreamingRetryClient;
import com.salesforce.scv.SCVLoggingUtil;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.transcribestreaming.model.AudioStream;
import software.amazon.awssdk.services.transcribestreaming.model.LanguageCode;
import software.amazon.awssdk.services.transcribestreaming.model.MediaEncoding;
import software.amazon.awssdk.services.transcribestreaming.model.TranscribeStreamingRequest;
import software.amazon.awssdk.services.transcribestreaming.model.StartStreamTranscriptionRequest;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Real KVS Transcriber Service for WebSocket output
 * This version uses actual Kinesis Video Streams integration
 */
public class RealKVSTranscribeStreamingService implements RequestHandler<TranscriptionRequest, String> {

    private static final String REGION = System.getenv("APP_REGION");
    private static final String START_SELECTOR_TYPE = System.getenv("START_SELECTOR_TYPE");
    public static final MetricsUtil metricsUtil = new MetricsUtil(CloudWatchClient.create());
    private WebSocketTranscribedSegmentWriter fromCustomerSegmentWriter = null;
    private WebSocketTranscribedSegmentWriter toCustomerSegmentWriter = null;

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
    public String handleRequest(TranscriptionRequest request, Context context) {
        Map<String, String> loggingContext = new HashMap<>();
        loggingContext.put(SCVLoggingUtil.VOICECALL_CONTEXT_KEY.VOICE_CALL_ID.toString(), request.getVoiceCallId());
        
        try {
            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.RealKVSTranscribeStreamingService.handleRequest", 
                    SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, "Start Handle Request", loggingContext);

            // validate the request
            request.validate();

            ConfigManager.SecretConfig config = ConfigManager.getSecretConfig(request.getSecretName());

            startRealTranscribeStreaming(request.getInstanceARN(), request.getStreamARN(), request.getStartFragmentNum(), 
                    request.getVoiceCallId(), request.getLanguageCode(), request.getAudioStartTimestamp(), 
                    request.getCustomerPhoneNumber(), request.isStreamAudioFromCustomer(), request.isStreamAudioToCustomer(), 
                    request.getEngine(), request.getVocabularyName(), request.getVocabularyFilterName(), 
                    request.getVocabularyFilterMethod(), request.getSpecialty(), config);

            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.RealKVSTranscribeStreamingService.handleRequest", 
                    SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, "End Handle Request", loggingContext);
            return "{ \"result\": \"Success\" }";
        } catch (Exception e) {
            SCVLoggingUtil.error("com.amazonaws.kvstranscribestreaming.RealKVSTranscribeStreamingService.handleRequest", 
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, e.getMessage(), loggingContext);
            return "{ \"result\": \"Failed\" }";
        }
    }

    /**
     * Real streaming implementation using actual KVS streams
     */
    private void startRealTranscribeStreaming(String instanceARN, String streamARN, String startFragmentNum, 
                                            String voiceCallId, Optional<String> languageCode, long audioStartTimestamp, 
                                            String customerPhoneNumber, boolean isStreamAudioFromCustomerEnabled, 
                                            boolean isStreamAudioToCustomerEnabled, String engine,
                                            Optional<String> vocabularyName, Optional<String> vocabularyFilterName, 
                                            Optional<String> vocabularyFilterMethod, Optional<String> specialty, 
                                            ConfigManager.SecretConfig config) throws Exception {

        SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.RealKVSTranscribeStreamingService.startRealTranscribeStreaming", 
                SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, "START Real Transcribe Streaming", null);

        SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.RealKVSTranscribeStreamingService.startRealTranscribeStreaming",
                SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                "Using configuration from secret: " + config.getSourceSecretName() + " for voiceCallId: " + voiceCallId,
                null);

        fromCustomerSegmentWriter = new WebSocketTranscribedSegmentWriter(instanceARN, voiceCallId, true, audioStartTimestamp, customerPhoneNumber, config);
        toCustomerSegmentWriter = new WebSocketTranscribedSegmentWriter(instanceARN, voiceCallId, false, audioStartTimestamp, customerPhoneNumber, config);

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

        SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.RealKVSTranscribeStreamingService.startRealTranscribeStreaming", 
                SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, "START Initialize Transcribe client ", null);
        
        try (TranscribeStreamingRetryClient client = new TranscribeStreamingRetryClient(getTranscribeCredentials(), transcribeEndpoint, 
                Region.of(transcribeRegion), metricsUtil)) {
            
            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.RealKVSTranscribeStreamingService.startRealTranscribeStreaming", 
                    SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, "END Initialize Transcribe client ", null);
            
            CompletableFuture<Void> fromCustomerResult = null;
            CompletableFuture<Void> toCustomerResult = null;

            if (fromCustomerStream != null) {
                SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.RealKVSTranscribeStreamingService.getStartStreamingTranscriptionFuture", 
                        SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, "START Get Transcribing Future for FROM_CUSTOMER stream", null);
                fromCustomerResult = getStartStreamingTranscriptionFuture(
                        fromCustomerStream, languageCode, voiceCallId, client, fromCustomerSegmentWriter, 
                        RealKVSUtils.TrackName.AUDIO_FROM_CUSTOMER.getName(), engine, 
                        vocabularyName, vocabularyFilterName, vocabularyFilterMethod, specialty);
            }

            if (toCustomerStream != null) {
                SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.RealKVSTranscribeStreamingService.getStartStreamingTranscriptionFuture", 
                        SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, "START Get Transcribing Future for TO_CUSTOMER stream", null);
                toCustomerResult = getStartStreamingTranscriptionFuture(
                        toCustomerStream, languageCode, voiceCallId, client, toCustomerSegmentWriter, 
                        RealKVSUtils.TrackName.AUDIO_TO_CUSTOMER.getName(), engine, 
                        vocabularyName, vocabularyFilterName, vocabularyFilterMethod, specialty);
            }

            // Synchronous wait for stream to close
            if (null != fromCustomerResult) {
                fromCustomerResult.get(890, TimeUnit.SECONDS);
            }

            if (null != toCustomerResult) {
                toCustomerResult.get(890, TimeUnit.SECONDS);
            }
        } catch (TimeoutException e) {
            SCVLoggingUtil.error("com.amazonaws.kvstranscribestreaming.RealKVSTranscribeStreamingService.startRealTranscribeStreaming", 
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, e.getMessage(), null);
        } catch (Exception e) {
            SCVLoggingUtil.error("com.amazonaws.kvstranscribestreaming.RealKVSTranscribeStreamingService.startRealTranscribeStreaming", 
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, e.getMessage(), null);
            throw e;
        } finally {
            // Clean up WebSocket connections
            if (fromCustomerSegmentWriter != null) {
                fromCustomerSegmentWriter.close();
            }
            if (toCustomerSegmentWriter != null) {
                toCustomerSegmentWriter.close();
            }
        }
        SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.RealKVSTranscribeStreamingService.startRealTranscribeStreaming", 
                SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, "END Real Transcribe Streaming", null);
    }

    private CompletableFuture<Void> getStartStreamingTranscriptionFuture(InputStream kvsInputStream, Optional<String> languageCodeOptional, 
                                                                         String contactId, TranscribeStreamingRetryClient client,
                                                                         WebSocketTranscribedSegmentWriter transcribedSegmentWriter, 
                                                                         String channel, String engine, Optional<String> vocabularyName,
                                                                         Optional<String> vocabularyFilterName, Optional<String> vocabularyFilterMethod, 
                                                                         Optional<String> specialty) {
        String languageCode = languageCodeOptional.isPresent() ? languageCodeOptional.get() : LanguageCode.EN_US.toString();
        TranscribeStreamingRequest request = getStandardRequest(languageCode, vocabularyName, vocabularyFilterName, vocabularyFilterMethod);

        return client.startStreamTranscription(
                request,
                new RealAudioStreamPublisher(kvsInputStream, contactId, channel),
                new StreamTranscriptionBehaviorImpl(transcribedSegmentWriter),
                channel,
                engine
        );
    }

    /**
     * Build StartStreamTranscriptionRequest containing required parameters
     */
    private static StartStreamTranscriptionRequest getStandardRequest(String languageCode, Optional<String> vocabularyName, 
                                                                     Optional<String> vocabularyFilterName, Optional<String> vocabularyFilterMethod) {
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
            builder.vocabularyFilterMethod(software.amazon.awssdk.services.transcribestreaming.model.VocabularyFilterMethod.fromValue(
                    vocabularyFilterMethod.isPresent() ? vocabularyFilterMethod.get() : 
                    software.amazon.awssdk.services.transcribestreaming.model.VocabularyFilterMethod.MASK.toString()));
        }
        return builder.build();
    }

    /**
     * Real Audio Stream Publisher that reads from actual KVS streams
     */
    private static class RealAudioStreamPublisher implements Publisher<AudioStream> {
        private InputStream kvsInputStream;
        private String contactId;
        private String channel;

        private RealAudioStreamPublisher(InputStream kvsInputStream, String contactId, String channel) {
            this.kvsInputStream = kvsInputStream;
            this.contactId = contactId;
            this.channel = channel;
        }

        @Override
        public void subscribe(Subscriber<? super AudioStream> s) {
            s.onSubscribe(new RealAudioEventSubscription(s, kvsInputStream, contactId, channel));
        }
    }

    /**
     * Real Audio Event Subscription that reads from actual KVS streams
     */
    private static class RealAudioEventSubscription implements org.reactivestreams.Subscription {
        private final Subscriber<? super AudioStream> subscriber;
        private InputStream kvsInputStream;
        private String contactId;
        private String channel;
        private java.util.concurrent.atomic.AtomicLong demand = new java.util.concurrent.atomic.AtomicLong(0);
        private java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(1);

        public RealAudioEventSubscription(Subscriber<? super AudioStream> s, InputStream kvsInputStream, String contactId, String channel) {
            this.subscriber = s;
            this.kvsInputStream = kvsInputStream;
            this.contactId = contactId;
            this.channel = channel;
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
                        // Read actual audio data from KVS stream
                        java.nio.ByteBuffer audioBuffer = RealKVSUtils.getByteBufferFromStream(kvsInputStream, contactId, 4, channel);

                        if (audioBuffer.remaining() > 0) {
                            software.amazon.awssdk.services.transcribestreaming.model.AudioEvent audioEvent = 
                                software.amazon.awssdk.services.transcribestreaming.model.AudioEvent.builder()
                                    .audioChunk(software.amazon.awssdk.core.SdkBytes.fromByteBuffer(audioBuffer))
                                    .build();
                            subscriber.onNext(audioEvent);
                        } else {
                            subscriber.onComplete();
                            break;
                        }
                        demand.getAndDecrement();
                    }
                } catch (Exception e) {
                    subscriber.onError(e);
                }
            });
        }

        @Override
        public void cancel() {
            executor.shutdown();
        }
    }

    /**
     * @return AWS credentials to be used to connect to all AWS services
     */
    private static AwsCredentialsProvider getTranscribeCredentials() {
        return DefaultCredentialsProvider.create();
    }
}
