package com.amazonaws.kvstranscribestreaming;

import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import java.time.Instant;

/*
 * <p>Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.</p>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
public class MetricsUtil {

    private static String NAMESPACE = "KVSTranscribeStreamingService";
    private final CloudWatchClient cloudWatchClient;

    public MetricsUtil(CloudWatchClient cloudWatchClient) {
        this.cloudWatchClient = cloudWatchClient;
    }

    public void recordMetric(final String metricName, long value) {
        MetricDatum metricData = MetricDatum.builder()
                .metricName(metricName)
                .timestamp(Instant.now())
                .unit(StandardUnit.COUNT)
                .value(Double.valueOf(value))
                .build();

        PutMetricDataRequest metricRequest = PutMetricDataRequest.builder()
                .namespace(NAMESPACE)
                .metricData(metricData)
                .build();

        cloudWatchClient.putMetricData(metricRequest);
    }
}
