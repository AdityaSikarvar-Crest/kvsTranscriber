package com.amazonaws.kvstranscribestreaming;

import com.amazonaws.transcribestreaming.StreamTranscriptionBehavior;
import com.salesforce.scv.SCVLoggingUtil;
import software.amazon.awssdk.services.transcribestreaming.model.TranscriptEvent;
import software.amazon.awssdk.services.transcribestreaming.model.TranscriptResultStream;
import software.amazon.awssdk.services.transcribestreaming.model.TranscribeStreamingResponse;
import software.amazon.awssdk.services.transcribestreaming.model.MedicalTranscriptEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class StreamTranscriptionBehaviorImpl implements StreamTranscriptionBehavior {

    private final TranscribedSegmentWriter transcribedSegmentWriter;

    public StreamTranscriptionBehaviorImpl(TranscribedSegmentWriter transcribedSegmentWriter) {
        this.transcribedSegmentWriter = transcribedSegmentWriter;
    }

    @Override
    public void onStandardStream(TranscriptEvent transcriptEvent) {
        SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.StreamTranscriptionBehaviorImpl.onStandardStream",
                SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                "Received transcript event with " + (transcriptEvent.transcript() != null ? transcriptEvent.transcript().results().size() : 0) + " results", null);
                
        if (transcriptEvent.transcript() != null && transcriptEvent.transcript().results() != null && !transcriptEvent.transcript().results().isEmpty()) {
            transcriptEvent.transcript().results().forEach(result -> {
                if (result.alternatives() != null && !result.alternatives().isEmpty()) {
                    String transcript = result.alternatives().get(0).transcript();
                    if (transcript != null && !transcript.trim().isEmpty()) {
                        String messageId = UUID.randomUUID().toString();
                        long startTime = result.startTime() != null ? (long)(result.startTime() * 1000) : System.currentTimeMillis();
                        long endTime = result.endTime() != null ? (long)(result.endTime() * 1000) : System.currentTimeMillis();
                        boolean isPartial = result.isPartial() != null ? result.isPartial() : false;

                        Map<String, String> loggingContext = new HashMap<>();
                        loggingContext.put(SCVLoggingUtil.TRANSCRIPTION_CONTEXT_KEY.MESSAGE_ID.toString(), messageId);
                        loggingContext.put(SCVLoggingUtil.TRANSCRIPTION_CONTEXT_KEY.START_TIME.toString(), String.valueOf(startTime));
                        loggingContext.put(SCVLoggingUtil.TRANSCRIPTION_CONTEXT_KEY.END_TIME.toString(), String.valueOf(endTime));
                        loggingContext.put("isPartial", String.valueOf(isPartial));

                        SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.StreamTranscriptionBehaviorImpl.onStandardStream", 
                                SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, 
                                "Processing transcript: '" + transcript + "' (partial: " + isPartial + ", length: " + transcript.length() + ")", loggingContext);

                        try {
                            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.StreamTranscriptionBehaviorImpl.onStandardStream",
                                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                                    "Writing transcript to composite writer", loggingContext);
                            transcribedSegmentWriter.write(transcript, messageId, startTime, endTime, isPartial);
                            SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.StreamTranscriptionBehaviorImpl.onStandardStream",
                                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                                    "Successfully wrote transcript to composite writer", loggingContext);
                        } catch (Exception e) {
                            SCVLoggingUtil.error("com.amazonaws.kvstranscribestreaming.StreamTranscriptionBehaviorImpl.onStandardStream", 
                                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, 
                                    "Error writing transcript: " + e.getMessage(), loggingContext);
                        }
                    } else {
                        SCVLoggingUtil.debug("com.amazonaws.kvstranscribestreaming.StreamTranscriptionBehaviorImpl.onStandardStream",
                                SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                                "Skipping empty transcript", null);
                    }
                } else {
                    SCVLoggingUtil.debug("com.amazonaws.kvstranscribestreaming.StreamTranscriptionBehaviorImpl.onStandardStream",
                            SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                            "No alternatives in transcript result", null);
                }
            });
        } else {
            SCVLoggingUtil.debug("com.amazonaws.kvstranscribestreaming.StreamTranscriptionBehaviorImpl.onStandardStream",
                    SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                    "No transcript results to process", null);
        }
    }

    @Override
    public void onResponse(TranscribeStreamingResponse response) {
        SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.StreamTranscriptionBehaviorImpl.onResponse", 
                SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, 
                "Transcription response received", null);
    }

    @Override
    public void onMedicalStream(MedicalTranscriptEvent medicalTranscriptEvent) {
        SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.StreamTranscriptionBehaviorImpl.onMedicalStream", 
                SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, 
                "Medical transcript event received", null);
        // Handle medical transcript events if needed
    }

    @Override
    public void onError(Throwable error) {
        SCVLoggingUtil.error("com.amazonaws.kvstranscribestreaming.StreamTranscriptionBehaviorImpl.onError", 
                SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION, 
                "Transcription stream error: " + error.getMessage(), null);
    }

    @Override
    public void onComplete() {
        SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.StreamTranscriptionBehaviorImpl.onComplete", 
                SCVLoggingUtil.EVENT_TYPE.PERFORMANCE, 
                "Transcription stream completed", null);
    }
}
