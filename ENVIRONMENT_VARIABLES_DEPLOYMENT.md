# KVS Transcriber WebSocket Deployment with Environment Variables

This guide provides exact commands and locations for deploying your WebSocket-based transcription service using environment variables instead of AWS Secrets Manager.

## Prerequisites Checklist

Before starting, ensure you have:
- [ ] AWS CLI installed and configured (`aws configure`)
- [ ] Maven installed (`mvn --version`)
- [ ] Java 11+ installed (`java --version`)
- [ ] Access to AWS Console
- [ ] WebSocket server running and accessible
- [ ] AWS account with appropriate permissions
- [ ] Existing IAM role for the Lambda function

## Step 1: Build the Project

**Location:** Open Command Prompt/PowerShell in your project directory

**Directory:** `C:\Users\AdityaSikarvar\Desktop\kvsTranscriber`

```bash
# Navigate to project directory
cd C:\Users\AdityaSikarvar\Desktop\kvsTranscriber

# Clean and build the project
mvn clean package

# Verify the JAR was created
dir target\kvs-transcriber-websocket-1.0.0.jar
```

**Expected Output:**
```
[INFO] BUILD SUCCESS
[INFO] Total time: XX.XXX s
[INFO] Finished at: YYYY-MM-DDTHH:MM:SS+XX:XX
[INFO] Final Memory: XXM/XXM
```

## Step 2: Deploy Lambda Function

**Location:** Command Prompt/PowerShell in project directory

**Get your AWS Account ID:**
```bash
# Get your AWS account ID
aws sts get-caller-identity --query Account --output text
```

**Deploy the Lambda function with environment variables:**
```bash
# Replace YOUR_ACCOUNT_ID with your actual account ID
# Replace YOUR_IAM_ROLE_NAME with your existing IAM role name
aws lambda create-function \
  --function-name kvs-transcriber-websocket \
  --runtime java11 \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/YOUR_IAM_ROLE_NAME \
  --handler com.amazonaws.kvstranscribestreaming.RealKVSTranscribeStreamingService \
  --zip-file fileb://target/kvs-transcriber-websocket-1.0.0.jar \
  --timeout 900 \
  --memory-size 1024 \
  --environment Variables='{
    "WEBSOCKET_ENDPOINT": "wss://your-websocket-server.com/transcription",
    "WEBSOCKET_CONNECTION_TIMEOUT_SECONDS": "30",
    "WEBSOCKET_MAX_RECONNECT_ATTEMPTS": "3",
    "TRANSCRIBE_REGION": "us-east-1",
    "APP_REGION": "us-east-1",
    "START_SELECTOR_TYPE": "FRAGMENT_NUMBER"
  }'
```

**Expected Output:**
```json
{
    "FunctionName": "kvs-transcriber-websocket",
    "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:kvs-transcriber-websocket",
    "Runtime": "java11",
    "Role": "arn:aws:iam::123456789012:role/YOUR_IAM_ROLE_NAME",
    "Handler": "com.amazonaws.kvstranscribestreaming.RealKVSTranscribeStreamingService",
    "CodeSize": 12345678,
    "Description": "",
    "Timeout": 900,
    "MemorySize": 1024,
    "LastModified": "2024-01-01T00:00:00.000+0000",
    "CodeSha256": "XXXXXXXXXXXXXX",
    "Version": "$LATEST",
    "Environment": {
        "Variables": {
            "WEBSOCKET_ENDPOINT": "wss://your-websocket-server.com/transcription",
            "WEBSOCKET_CONNECTION_TIMEOUT_SECONDS": "30",
            "WEBSOCKET_MAX_RECONNECT_ATTEMPTS": "3",
            "TRANSCRIBE_REGION": "us-east-1",
            "APP_REGION": "us-east-1",
            "START_SELECTOR_TYPE": "FRAGMENT_NUMBER"
        }
    }
}
```

## Step 3: Update Environment Variables (if needed)

**Location:** Command Prompt/PowerShell

**Update environment variables:**
```bash
aws lambda update-function-configuration \
  --function-name kvs-transcriber-websocket \
  --environment Variables='{
    "WEBSOCKET_ENDPOINT": "wss://your-actual-websocket-server.com:8080",
    "WEBSOCKET_CONNECTION_TIMEOUT_SECONDS": "30",
    "WEBSOCKET_MAX_RECONNECT_ATTEMPTS": "3",
    "TRANSCRIBE_REGION": "us-east-1",
    "APP_REGION": "us-east-1",
    "START_SELECTOR_TYPE": "FRAGMENT_NUMBER"
  }'
```

## Step 4: Test the Lambda Function

**Location:** Command Prompt/PowerShell in project directory

**Create test event file:**
```bash
# Create test event file
echo {
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
  "secretName": "dummy-secret-name"
} > test-event.json
```

**Test the function:**
```bash
# Test the function
aws lambda invoke \
  --function-name kvs-transcriber-websocket \
  --payload file://test-event.json \
  --cli-binary-format raw-in-base64-out \
  response.json

# View the response
type response.json
```

**Expected Output:**
```json
{
    "StatusCode": 200,
    "ExecutedVersion": "$LATEST"
}
```

## Step 5: Set Up KVS Consumer Trigger (if not exists)

**Location:** AWS Console

1. Go to [Lambda Console](https://console.aws.amazon.com/lambda/)
2. Click "Create function"
3. Choose "Author from scratch"
4. Function name: `kvs-consumer-trigger`
5. Runtime: Java 11
6. Role: Use your existing IAM role
7. Click "Create function"
8. Upload the same JAR file: `target/kvs-transcriber-websocket-1.0.0.jar`
9. Handler: `com.amazonaws.kvstranscribestreaming.KVSConsumerTrigger`

## Step 6: Configure KVS Stream Consumer

**Location:** AWS Console

1. Go to [Kinesis Video Streams Console](https://console.aws.amazon.com/kinesisvideo/)
2. Select your stream
3. Go to "Data consumers" tab
4. Click "Add consumer"
5. Consumer type: "Lambda function"
6. Select `kvs-consumer-trigger` function
7. Click "Add"

## Step 7: Set Up WebSocket Server

**Location:** Your server environment

**Create a simple WebSocket server (Node.js example):**

```bash
# Create package.json
echo {
  "name": "transcription-websocket-server",
  "version": "1.0.0",
  "description": "WebSocket server for KVS transcriptions",
  "main": "server.js",
  "scripts": {
    "start": "node server.js"
  },
  "dependencies": {
    "ws": "^8.14.2"
  }
} > package.json

# Install dependencies
npm install

# Create server.js
echo const WebSocket = require('ws');

const wss = new WebSocket.Server({ port: 8080 });

console.log('WebSocket server running on port 8080');

wss.on('connection', function connection(ws, req) {
  console.log('New transcription client connected');
  
  ws.on('message', function incoming(data) {
    try {
      const transcription = JSON.parse(data);
      console.log('Received transcription:', JSON.stringify(transcription, null, 2));
      
      // Broadcast to all connected clients
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
  
  ws.on('error', function error(err) {
    console.error('WebSocket error:', err);
  });
}); > server.js

# Start the server
npm start
```

## Step 8: Update WebSocket Endpoint

**Location:** Command Prompt/PowerShell

**Update the environment variable with your actual WebSocket URL:**
```bash
aws lambda update-function-configuration \
  --function-name kvs-transcriber-websocket \
  --environment Variables='{
    "WEBSOCKET_ENDPOINT": "wss://your-actual-server.com:8080",
    "WEBSOCKET_CONNECTION_TIMEOUT_SECONDS": "30",
    "WEBSOCKET_MAX_RECONNECT_ATTEMPTS": "3",
    "TRANSCRIBE_REGION": "us-east-1",
    "APP_REGION": "us-east-1",
    "START_SELECTOR_TYPE": "FRAGMENT_NUMBER"
  }'
```

## Step 9: Monitor and Verify

**Location:** AWS Console

### Check Lambda Logs:
1. Go to [CloudWatch Logs](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups)
2. Find log group: `/aws/lambda/kvs-transcriber-websocket`
3. Check for WebSocket connection logs

### Check Lambda Metrics:
1. Go to [Lambda Console](https://console.aws.amazon.com/lambda/)
2. Select `kvs-transcriber-websocket` function
3. Go to "Monitoring" tab
4. Check for errors and duration metrics

### Test End-to-End:
1. Start your WebSocket server
2. Trigger a call through Amazon Connect
3. Check WebSocket server logs for incoming transcriptions
4. Verify transcription data format

## Environment Variables Reference

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `WEBSOCKET_ENDPOINT` | WebSocket server URL | Yes | - |
| `WEBSOCKET_CONNECTION_TIMEOUT_SECONDS` | Connection timeout | No | 30 |
| `WEBSOCKET_MAX_RECONNECT_ATTEMPTS` | Max reconnection attempts | No | 3 |
| `TRANSCRIBE_REGION` | AWS Transcribe region | No | us-east-1 |
| `APP_REGION` | AWS application region | No | us-east-1 |
| `START_SELECTOR_TYPE` | KVS start selector type | No | FRAGMENT_NUMBER |

## Troubleshooting Commands

**Location:** Command Prompt/PowerShell

**Check Lambda function status:**
```bash
aws lambda get-function --function-name kvs-transcriber-websocket
```

**Check recent logs:**
```bash
aws logs describe-log-streams --log-group-name /aws/lambda/kvs-transcriber-websocket --order-by LastEventTime --descending --max-items 1
```

**Update function code (if needed):**
```bash
aws lambda update-function-code \
  --function-name kvs-transcriber-websocket \
  --zip-file fileb://target/kvs-transcriber-websocket-1.0.0.jar
```

**Check environment variables:**
```bash
aws lambda get-function-configuration --function-name kvs-transcriber-websocket --query Environment
```

## Required IAM Permissions

Your existing IAM role should have these permissions:

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

## Next Steps After Deployment

1. **Test with real Amazon Connect calls**
2. **Monitor WebSocket server logs**
3. **Set up CloudWatch alarms for errors**
4. **Configure auto-scaling if needed**
5. **Set up monitoring dashboards**

This completes the deployment process using environment variables instead of AWS Secrets Manager. The configuration is now simpler and more straightforward!
