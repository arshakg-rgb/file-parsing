console.log('start');
const { SQSClient, GetQueueUrlCommand, GetQueueAttributesCommand } = await import('@aws-sdk/client-sqs');
console.log('imported');
const sqs = new SQSClient({ endpoint: 'http://localhost:4566', region: 'us-east-1', credentials: { accessKeyId: 'test', secretAccessKey: 'test' } });
console.log('client');
const { QueueUrl } = await sqs.send(new GetQueueUrlCommand({ QueueName: 'fpp-parse.fifo' }));
console.log('url', QueueUrl);
const { Attributes } = await sqs.send(new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ['All'] }));
console.log(JSON.stringify(Attributes, null, 2));
