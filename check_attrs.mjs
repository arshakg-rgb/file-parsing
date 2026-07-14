import { SQSClient, GetQueueUrlCommand, GetQueueAttributesCommand } from './node_modules/@aws-sdk/client-sqs/dist-cjs/index.js';
const sqs = new SQSClient({ endpoint: 'http://localhost:4566', region: 'us-east-1', credentials: { accessKeyId: 'test', secretAccessKey: 'test' } });
const { QueueUrl } = await sqs.send(new GetQueueUrlCommand({ QueueName: 'fpp-parse.fifo' }));
const { Attributes } = await sqs.send(new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ['All'] }));
console.log(JSON.stringify(Attributes, null, 2));
