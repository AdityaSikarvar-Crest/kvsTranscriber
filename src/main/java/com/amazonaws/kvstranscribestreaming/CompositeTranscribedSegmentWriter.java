package com.amazonaws.kvstranscribestreaming;

import com.salesforce.scv.SCVLoggingUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * CompositeTranscribedSegmentWriter sends transcriptions to multiple writers
 * This allows sending to both DynamoDB and WebSocket simultaneously
 */
public class CompositeTranscribedSegmentWriter implements TranscribedSegmentWriter {

    private final List<TranscribedSegmentWriter> writers;

    public CompositeTranscribedSegmentWriter(TranscribedSegmentWriter... writers) {
        this.writers = new ArrayList<>();
        for (TranscribedSegmentWriter writer : writers) {
            if (writer != null) {
                this.writers.add(writer);
            }
        }
        
        SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.CompositeTranscribedSegmentWriter.constructor",
                SCVLoggingUtil.EVENT_TYPE.PERFORMANCE,
                "Initialized composite writer with " + this.writers.size() + " writers", null);
    }

    @Override
    public void write(String message, String messageId, long startTime, long endTime, boolean isPartial) {
        SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.CompositeTranscribedSegmentWriter.write",
                SCVLoggingUtil.EVENT_TYPE.PERFORMANCE,
                "Writing transcript to " + writers.size() + " writers (partial: " + isPartial + ")", null);

        // Write to all writers
        for (TranscribedSegmentWriter writer : writers) {
            try {
                writer.write(message, messageId, startTime, endTime, isPartial);
                SCVLoggingUtil.debug("com.amazonaws.kvstranscribestreaming.CompositeTranscribedSegmentWriter.write",
                        SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                        "Successfully wrote to writer: " + writer.getClass().getSimpleName(), null);
            } catch (Exception e) {
                SCVLoggingUtil.error("com.amazonaws.kvstranscribestreaming.CompositeTranscribedSegmentWriter.write",
                        SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                        "Failed to write to writer " + writer.getClass().getSimpleName() + ": " + e.getMessage(), null);
                // Continue with other writers even if one fails
            }
        }
    }

    @Override
    public void close() {
        SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.CompositeTranscribedSegmentWriter.close",
                SCVLoggingUtil.EVENT_TYPE.PERFORMANCE,
                "Closing " + writers.size() + " writers", null);

        // Close all writers
        for (TranscribedSegmentWriter writer : writers) {
            try {
                writer.close();
                SCVLoggingUtil.debug("com.amazonaws.kvstranscribestreaming.CompositeTranscribedSegmentWriter.close",
                        SCVLoggingUtil.EVENT_TYPE.PERFORMANCE,
                        "Successfully closed writer: " + writer.getClass().getSimpleName(), null);
            } catch (Exception e) {
                SCVLoggingUtil.error("com.amazonaws.kvstranscribestreaming.CompositeTranscribedSegmentWriter.close",
                        SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                        "Failed to close writer " + writer.getClass().getSimpleName() + ": " + e.getMessage(), null);
            }
        }
    }
}
