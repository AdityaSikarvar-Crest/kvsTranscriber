# KVS Transcriber WebSocket Deployment Guide

This guide will help you deploy the modified KVS Transcriber Lambda function that sends transcriptions directly to a WebSocket URL instead of Salesforce SCRT.

## Prerequisites

1. **AWS CLI** configured with appropriate permissions
2. **Maven** installed for building the project
3. **Java 11** or higher
4. **WebSocket server** running and accessible
5. **AWS Secrets Manager** access for configuration storage

## Step 1: Build the Project

```bash
# Navigate to the project directory
cd /c/Users/AdityaSikarvar/Desktop/kvsTranscriber

# Clean and build the project
mvn clean package

# The JAR file will be created in target/kvs-transcriber-websocket-1.0.0.jar
```

## Step 2: Configure AWS Secrets Manager

1. **Create a new secret in AWS Secrets Manager:**
   ```bash
   aws secretsmanager create-secret \
     --name "kvs-transcriber-websocket-config" \
     --description "Configuration for KVS Transcriber WebSocket service" \
     --secret-string file://websocket-config-example.json
   ```

2. **Update the secret with your actual WebSocket endpoint:**
   ```bash
   aws secretsmanager update-secret \
     --secret-id "kvs-transcriber-websocket-config" \
     --secret-string '{
       "WEBSOCKET_ENDPOINT": "wss://your-actual-websocket-server.com/transcription",
       "WEBSOCKET_CONNECTION_TIMEOUT_SECONDS": "30",
       "WEBSOCKET_MAX_RECONNECT_ATTEMPTS": "3",
       "TRANSCRIBE_REGION": "us-east-1",
       "APP_REGION": "us-east-1",
       "START_SELECTOR_TYPE": "FRAGMENT_NUMBER"
     }'
   ```

## Step 3: Create IAM Role for Lambda

1. **Create trust policy file (`lambda-trust-policy.json`):**
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": {
           "Service": "lambda.amazonaws.com"
         },
         "Action": "sts:AssumeRole"
       }
     ]
   }
   ```

2. **Create IAM role:**
   ```bash
   aws iam create-role \
     --role-name KVS-Transcriber-WebSocket-Role \
     --assume-role-policy-document file://lambda-trust-policy.json
   ```

3. **Create IAM policy file (`lambda-policy.json`):**
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "logs:CreateLogGroup",
           "logs:CreateLogStream",
           "logs:PutLogEvents"
         ],
         "Resource": "arn:aws:logs:*:*:*"
       },
       {
         "Effect": "Allow",
         "Action": [
           "kinesisvideo:GetDataEndpoint",
           "kinesisvideo:GetMedia"
         ],
         "Resource": "*"
       },
       {
         "Effect": "Allow",
         "Action": [
           "transcribestreaming:StartStreamTranscription",
           "transcribestreaming:StartMedicalStreamTranscription"
         ],
         "Resource": "*"
       },
       {
         "Effect": "Allow",
         "Action": [
           "secretsmanager:GetSecretValue"
         ],
         "Resource": "arn:aws:secretsmanager:*:*:secret:kvs-transcriber-websocket-config*"
       },
       {
         "Effect": "Allow",
         "Action": [
           "cloudwatch:PutMetricData"
         ],
         "Resource": "*"
       },
       {
         "Effect": "Allow",
         "Action": [
           "connect:UpdateContactAttributes"
         ],
         "Resource": "*"
       }
     ]
   }
   ```

4. **Attach policy to role:**
   ```bash
   aws iam put-role-policy \
     --role-name KVS-Transcriber-WebSocket-Role \
     --policy-name KVS-Transcriber-WebSocket-Policy \
     --policy-document file://lambda-policy.json
   ```

## Step 4: Deploy Lambda Function

1. **Create Lambda function:**
   ```bash
   aws lambda create-function \
     --function-name kvs-transcriber-websocket \
     --runtime java11 \
     --role arn:aws:iam::YOUR_ACCOUNT_ID:role/KVS-Transcriber-WebSocket-Role \
     --handler com.amazonaws.kvstranscribestreaming.KVSTranscribeStreamingService \
     --zip-file fileb://target/kvs-transcriber-websocket-1.0.0.jar \
     --timeout 900 \
     --memory-size 1024 \
     --environment Variables='{APP_REGION=us-east-1,START_SELECTOR_TYPE=FRAGMENT_NUMBER}'
   ```

2. **Update function code (if redeploying):**
   ```bash
   aws lambda update-function-code \
     --function-name kvs-transcriber-websocket \
     --zip-file fileb://target/kvs-transcriber-websocket-1.0.0.jar
   ```

## Step 5: Configure Lambda Environment Variables

```bash
aws lambda update-function-configuration \
  --function-name kvs-transcriber-websocket \
  --environment Variables='{
    APP_REGION=us-east-1,
    START_SELECTOR_TYPE=FRAGMENT_NUMBER
  }'
```

## Step 6: Test the Lambda Function

1. **Create test event (`test-event.json`):**
   ```json
   {
     "streamARN": "arn:aws:kinesisvideo:us-east-1:123456789012:stream/your-stream-name/1234567890",
     "startFragmentNum": "1234567890",
     "audioStartTimestamp": "1640995200000",
     "customerPhoneNumber": "+1234567890",
     "voiceCallId": "test-call-123",
     "languageCode": "en-US",
     "streamAudioFromCustomer": true,
     "streamAudioToCustomer": true,
     "instanceARN": "arn:aws:connect:us-east-1:123456789012:instance/your-instance-id",
     "engine": "standard",
     "secretName": "kvs-transcriber-websocket-config"
   }
   ```

2. **Test the function:**
   ```bash
   aws lambda invoke \
     --function-name kvs-transcriber-websocket \
     --payload file://test-event.json \
     --cli-binary-format raw-in-base64-out \
     response.json
   ```

## Step 7: Set Up KVS Consumer Trigger

1. **Create KVS Consumer Lambda function** (if not already exists):
   ```bash
   aws lambda create-function \
     --function-name kvs-consumer-trigger \
     --runtime java11 \
     --role arn:aws:iam::YOUR_ACCOUNT_ID:role/KVS-Transcriber-WebSocket-Role \
     --handler com.amazonaws.kvstranscribestreaming.KVSConsumerTrigger \
     --zip-file fileb://target/kvs-transcriber-websocket-1.0.0.jar \
     --timeout 60 \
     --memory-size 256
   ```

2. **Configure KVS stream to trigger the consumer:**
   - Go to Amazon Kinesis Video Streams console
   - Select your stream
   - Go to "Data consumers" tab
   - Add a Lambda function consumer
   - Select `kvs-consumer-trigger` function

## Step 8: WebSocket Server Setup

Your WebSocket server should handle the following message format:

```json
{
  "voiceCallId": "call-123",
  "participantId": "+1234567890",
  "messageId": "msg-456",
  "startTime": 1640995200000,
  "endTime": 1640995201000,
  "content": "Hello, how can I help you?",
  "senderType": "CUSTOMER",
  "isPartial": false,
  "timestamp": 1640995200000,
  "instanceARN": "arn:aws:connect:us-east-1:123456789012:instance/your-instance-id"
}
```

### WebSocket Server Example (Node.js)

```javascript
const WebSocket = require('ws');

const wss = new WebSocket.Server({ port: 8080 });

wss.on('connection', function connection(ws, req) {
  console.log('New transcription client connected');
  
  ws.on('message', function incoming(data) {
    try {
      const transcription = JSON.parse(data);
      console.log('Received transcription:', transcription);
      
      // Broadcast to all connected clients or handle as needed
      wss.clients.forEach(function each(client) {
        if (client !== ws && client.readyState === WebSocket.OPEN) {
          client.send(data);
        }
      });
    } catch (error) {
      console.error('Error parsing transcription:', error);
    }
  });
  
  ws.on('close', function close() {
    console.log('Transcription client disconnected');
  });
});
```

## Step 9: Monitoring and Troubleshooting

1. **CloudWatch Logs:**
   - Check `/aws/lambda/kvs-transcriber-websocket` log group
   - Look for WebSocket connection errors
   - Monitor transcription processing logs

2. **CloudWatch Metrics:**
   - Custom metrics are published to `KVSTranscribeStreamingService` namespace
   - Monitor `TranscribeStreamError` metric for failures

3. **Common Issues:**
   - **WebSocket connection timeout**: Check if your WebSocket server is accessible
   - **Permission denied**: Verify IAM role has correct permissions
   - **Secret not found**: Ensure secret name matches in the request
   - **Memory issues**: Increase Lambda memory if needed

## Step 10: Performance Optimization

1. **Lambda Configuration:**
   - Memory: 1024MB (adjust based on usage)
   - Timeout: 900 seconds (15 minutes max)
   - Concurrent executions: Set appropriate limit

2. **WebSocket Configuration:**
   - Connection timeout: 30 seconds
   - Max reconnect attempts: 3
   - Consider using connection pooling for high volume

3. **Transcription Settings:**
   - Partial results are enabled by default for faster display
   - Adjust chunk size in `KVSByteToAudioEventSubscription` if needed

## Security Considerations

1. **WebSocket Security:**
   - Use WSS (secure WebSocket) in production
   - Implement authentication/authorization
   - Consider rate limiting

2. **AWS Security:**
   - Use least privilege IAM policies
   - Encrypt secrets in AWS Secrets Manager
   - Enable VPC endpoints if needed

3. **Data Privacy:**
   - Consider encrypting transcription data in transit
   - Implement data retention policies
   - Ensure compliance with privacy regulations

## Cost Optimization

1. **Lambda Costs:**
   - Monitor execution duration and memory usage
   - Use provisioned concurrency for predictable workloads
   - Consider ARM-based Graviton2 processors

2. **Transcribe Costs:**
   - Monitor transcription usage
   - Use appropriate language models
   - Consider batch processing for non-real-time use cases

## Support and Maintenance

1. **Logging:**
   - All operations are logged with structured logging
   - Use CloudWatch Insights for log analysis
   - Set up log retention policies

2. **Updates:**
   - Test changes in development environment first
   - Use Lambda versions and aliases for rollbacks
   - Monitor metrics after deployments

This deployment guide should get your WebSocket-based transcription service up and running. Make sure to test thoroughly in a development environment before deploying to production.
