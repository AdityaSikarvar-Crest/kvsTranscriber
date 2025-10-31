package com.amazonaws.kvstranscribestreaming;

import org.apache.commons.lang3.Validate;
import org.json.simple.JSONObject;
import com.salesforce.scv.SCVLoggingUtil;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.net.http.WebSocket.Listener;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * WebSocketTranscribedSegmentWriter sends transcriptions directly to a WebSocket URL
 * Supports both partial and final transcriptions for real-time display
 */
public class WebSocketTranscribedSegmentWriter implements TranscribedSegmentWriter {

    private final String voiceCallId;
    private final boolean isFromCustomer;
    long audioStartTimestamp;
    String customerPhoneNumber;
    private String instanceARN = null;
    private WebSocket webSocket;
    private final String websocketUrl;
    private final HttpClient httpClient;
    private CompletableFuture<WebSocket> webSocketFuture;

    // Config values extracted once in constructor
    private final String websocketEndpoint;
    private final int connectionTimeoutSeconds;
    private final int maxReconnectAttempts;

    public WebSocketTranscribedSegmentWriter(String instanceARN, String voiceCallId, boolean isFromCustomer, 
                                           long audioStartTimestamp, String customerPhoneNumber, 
                                           ConfigManager.SecretConfig config) {
        this.voiceCallId = Validate.notNull(voiceCallId);
        this.isFromCustomer = isFromCustomer;
        this.audioStartTimestamp = audioStartTimestamp;
        this.customerPhoneNumber = customerPhoneNumber;
        this.instanceARN = instanceARN;

        this.websocketEndpoint = config.getConfigValue("WEBSOCKET_ENDPOINT");
        this.connectionTimeoutSeconds = Integer.parseInt(config.getConfigValue("WEBSOCKET_CONNECTION_TIMEOUT_SECONDS"));
        this.maxReconnectAttempts = Integer.parseInt(config.getConfigValue("WEBSOCKET_MAX_RECONNECT_ATTEMPTS"));
        
        this.websocketUrl = websocketEndpoint + "?voiceCallId=" + voiceCallId + "&channel=" + 
                           (isFromCustomer ? "customer" : "agent");
        
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(connectionTimeoutSeconds))
                .build();

        SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.WebSocketTranscribedSegmentWriter.constructor",
                SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                "Using WebSocket endpoint: " + websocketUrl,
                null);

        // Initialize WebSocket connection immediately
        this.webSocketFuture = connectWebSocket();
        
        // Wait for initial connection to be established
        try {
            this.webSocket = webSocketFuture.get(5, TimeUnit.SECONDS);
            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.WebSocketTranscribedSegmentWriter.constructor",
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                    "WebSocket connection established immediately for voiceCallId: " + voiceCallId, null);
        } catch (Exception e) {
            SCVLoggingUtil.warn("com.amazonaws.kvstranscribestreaming.WebSocketTranscribedSegmentWriter.constructor",
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                    "WebSocket connection not immediately available, will retry on first message: " + e.getMessage(), null);
        }
    }

    private CompletableFuture<WebSocket> connectWebSocket() {
        return httpClient.newWebSocketBuilder()
                .buildAsync(URI.create(websocketUrl), new WebSocketListener())
                .orTimeout(connectionTimeoutSeconds, TimeUnit.SECONDS);
    }

    public void sendStandardRealTimeTranscript(software.amazon.awssdk.services.transcribestreaming.model.TranscriptEvent transcriptEvent) {
        List<software.amazon.awssdk.services.transcribestreaming.model.Result> results = transcriptEvent.transcript().results();
        if (results.size() > 0) {
            software.amazon.awssdk.services.transcribestreaming.model.Result result = results.get(0);

            // Send both partial and final results for real-time display
            if (result.alternatives().size() > 0 && !result.alternatives().get(0).transcript().isEmpty()) {
                String message = result.alternatives().get(0).transcript();
                String messageId = result.resultId();
                boolean isPartial = result.isPartial();

                // Calculate timestamps
                long startTime = Math.round(this.audioStartTimestamp + result.startTime() * 1000);
                long endTime = Math.round(this.audioStartTimestamp + result.endTime() * 1000);

                // Send message via WebSocket
                sendMessage(message, messageId, startTime, endTime, isPartial);
            }
        }
    }

    public void sendMedicalRealTimeTranscript(software.amazon.awssdk.services.transcribestreaming.model.MedicalTranscriptEvent transcriptEvent) {
        List<software.amazon.awssdk.services.transcribestreaming.model.MedicalResult> results = transcriptEvent.transcript().results();
        if (results.size() > 0) {
            software.amazon.awssdk.services.transcribestreaming.model.MedicalResult result = results.get(0);

            // Send both partial and final results for real-time display
            if (result.alternatives().size() > 0 && !result.alternatives().get(0).transcript().isEmpty()) {
                String message = result.alternatives().get(0).transcript();
                String messageId = result.resultId();
                boolean isPartial = result.isPartial();

                // Calculate timestamps
                long startTime = Math.round(this.audioStartTimestamp + result.startTime() * 1000);
                long endTime = Math.round(this.audioStartTimestamp + result.endTime() * 1000);

                // Send message via WebSocket
                sendMessage(message, messageId, startTime, endTime, isPartial);
            }
        }
    }

    /**
     * Implementation of TranscribedSegmentWriter interface
     */
    @Override
    public void write(String message, String messageId, long startTime, long endTime, boolean isPartial) {
        sendMessage(message, messageId, startTime, endTime, isPartial);
    }

    /**
     * Send transcription message via WebSocket
     */
    public void sendMessage(String message, String messageId, long startTime, long endTime, boolean isPartial) {
        SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.WebSocketTranscribedSegmentWriter.sendMessage", 
                SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, 
                "Sending message " + messageId + " (partial: " + isPartial + ")", null);

        // Get sender type and sender
        String senderType = this.isFromCustomer ? "CUSTOMER" : "AGENT";
        String sender = this.isFromCustomer ? customerPhoneNumber : voiceCallId;

        // Create JSON payload
        JSONObject messagePayload = new JSONObject();
        messagePayload.put("voiceCallId", voiceCallId);
        messagePayload.put("participantId", sender);
        messagePayload.put("messageId", messageId);
        messagePayload.put("startTime", Long.valueOf(startTime));
        messagePayload.put("endTime", Long.valueOf(endTime));
        messagePayload.put("content", message);
        messagePayload.put("senderType", senderType);
        messagePayload.put("isPartial", Boolean.valueOf(isPartial));
        messagePayload.put("timestamp", System.currentTimeMillis());
        messagePayload.put("instanceARN", instanceARN);

        // Send via WebSocket
        sendWebSocketMessage(messagePayload.toJSONString());

        // Log the response
        HashMap<String, String> loggingContextMap = new HashMap<>();
        loggingContextMap.put(SCVLoggingUtil.TRANSCRIPTION_CONTEXT_KEY.MESSAGE_ID.toString(), messageId);
        loggingContextMap.put(SCVLoggingUtil.TRANSCRIPTION_CONTEXT_KEY.START_TIME.toString(), String.valueOf(startTime));
        loggingContextMap.put(SCVLoggingUtil.TRANSCRIPTION_CONTEXT_KEY.END_TIME.toString(), String.valueOf(endTime));
        loggingContextMap.put(SCVLoggingUtil.TRANSCRIPTION_CONTEXT_KEY.END_POINT.toString(), websocketUrl);
        loggingContextMap.put("isPartial", String.valueOf(isPartial));
        
        SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.WebSocketTranscribedSegmentWriter.sendMessage", 
                SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, 
                "Message sent successfully", loggingContextMap);
    }

    private void sendWebSocketMessage(String message) {
        try {
            // Ensure WebSocket is connected
            if (webSocket == null || webSocket.isOutputClosed()) {
                SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.WebSocketTranscribedSegmentWriter.sendWebSocketMessage",
                        SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                        "WebSocket not connected, attempting to get connection", null);
                
                // Try to get existing connection first
                if (webSocketFuture != null && !webSocketFuture.isDone()) {
                    webSocket = webSocketFuture.get(2, TimeUnit.SECONDS);
                } else {
                    // Create new connection if needed
                    this.webSocketFuture = connectWebSocket();
                    webSocket = webSocketFuture.get(connectionTimeoutSeconds, TimeUnit.SECONDS);
                }
            }

            // Send text message immediately
            if (webSocket != null && !webSocket.isOutputClosed()) {
                webSocket.sendText(message, true);
                
                SCVLoggingUtil.debug("com.amazonaws.kvstranscribestreaming.WebSocketTranscribedSegmentWriter.sendWebSocketMessage",
                        SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                        "WebSocket message sent successfully: " + message.substring(0, Math.min(100, message.length())), null);
            } else {
                SCVLoggingUtil.warn("com.amazonaws.kvstranscribestreaming.WebSocketTranscribedSegmentWriter.sendWebSocketMessage",
                        SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                        "WebSocket is null or closed, cannot send message", null);
            }
            
        } catch (InterruptedException | java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            SCVLoggingUtil.error("com.amazonaws.kvstranscribestreaming.WebSocketTranscribedSegmentWriter.sendWebSocketMessage", 
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, 
                    "Failed to send WebSocket message: " + e.getMessage(), null);
            
            // Attempt to reconnect
            reconnectWebSocket();
        }
    }

    private void reconnectWebSocket() {
        try {
            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.WebSocketTranscribedSegmentWriter.reconnectWebSocket", 
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, 
                    "Attempting to reconnect WebSocket", null);
            
            this.webSocketFuture = connectWebSocket();
            this.webSocket = webSocketFuture.get(connectionTimeoutSeconds, TimeUnit.SECONDS);
            
        } catch (InterruptedException | java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            SCVLoggingUtil.error("com.amazonaws.kvstranscribestreaming.WebSocketTranscribedSegmentWriter.reconnectWebSocket", 
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, 
                    "Failed to reconnect WebSocket: " + e.getMessage(), null);
        }
    }

    public void close() {
        try {
            if (webSocket != null && !webSocket.isOutputClosed()) {
                webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "Transcription completed");
            }
        } catch (Exception e) {
            SCVLoggingUtil.error("com.amazonaws.kvstranscribestreaming.WebSocketTranscribedSegmentWriter.close", 
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, e.getMessage(), null);
        }
    }

    /**
     * WebSocket Listener to handle connection events
     */
    private class WebSocketListener implements Listener {
        @Override
        public void onOpen(WebSocket webSocket) {
            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.WebSocketTranscribedSegmentWriter.onOpen", 
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, 
                    "WebSocket connection opened for voiceCallId: " + voiceCallId, null);
            // Store the WebSocket reference for sending messages
            WebSocketTranscribedSegmentWriter.this.webSocket = webSocket;
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            SCVLoggingUtil.debug("com.amazonaws.kvstranscribestreaming.WebSocketTranscribedSegmentWriter.onText", 
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, 
                    "Received WebSocket message: " + data.toString(), null);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.WebSocketTranscribedSegmentWriter.onClose", 
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, 
                    "WebSocket connection closed. Status: " + statusCode + ", Reason: " + reason, null);
            // Clear the WebSocket reference
            WebSocketTranscribedSegmentWriter.this.webSocket = null;
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            SCVLoggingUtil.error("com.amazonaws.kvstranscribestreaming.WebSocketTranscribedSegmentWriter.onError", 
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, 
                    "WebSocket error: " + error.getMessage(), null);
            // Clear the WebSocket reference on error
            WebSocketTranscribedSegmentWriter.this.webSocket = null;
        }

        @Override
        public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
            return null;
        }

        @Override
        public CompletionStage<?> onPing(WebSocket webSocket, ByteBuffer message) {
            return null;
        }

        @Override
        public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
            return null;
        }
    }
}
