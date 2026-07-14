import { SQSClient, GetQueueAttributesCommand, GetQueueUrlCommand, ReceiveMessageCommand, DeleteMessageCommand } from "@aws-sdk/client-sqs";
const sqs = new SQSClient({ endpoint: "http://localhost:4566", region: "us-east-1", credentials: { accessKeyId: "test", secretAccessKey: "test" } });
const url = await sqs.send(new GetQueueUrlCommand({ QueueName: "fpp-parse.fifo" }));
console.log("QueueUrl:", url.QueueUrl);
const attrs = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: url.QueueUrl, AttributeNames: ["All"] }));
console.log("Attributes:", attrs.Attributes);
const msgs = await sqs.send(new ReceiveMessageCommand({ QueueUrl: url.QueueUrl, MaxNumberOfMessages: 10, VisibilityTimeout: 0, WaitTimeSeconds: 0 }));
console.log("Messages:", JSON.stringify(msgs.Messages, null, 2));
