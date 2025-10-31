package com.amazonaws.kvstranscribestreaming;

/**
 * Interface for writing transcribed segments
 */
public interface TranscribedSegmentWriter {
    
    /**
     * Write a transcribed segment
     * @param transcript The transcript text
     * @param messageId Unique message identifier
     * @param startTime Start time in milliseconds
     * @param endTime End time in milliseconds
     * @param isPartial Whether this is a partial transcript
     */
    void write(String transcript, String messageId, long startTime, long endTime, boolean isPartial);
    
    /**
     * Close the writer and cleanup resources
     */
    void close();
}