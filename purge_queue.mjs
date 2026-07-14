import { SQSClient, GetQueueUrlCommand, PurgeQueueCommand } from '@aws-sdk/client-sqs';
const sqs = new SQSClient({ endpoint: 'http://localhost:4566', region: 'us-east-1', credentials: { accessKeyId: 'test', secretAccessKey: 'test' } });
const { QueueUrl } = await sqs.send(new GetQueueUrlCommand({ QueueName: 'fpp-parse.fifo' }));
await sqs.send(new PurgeQueueCommand({ QueueUrl }));
console.log('PurgeQueue sent');
