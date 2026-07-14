import { CloudWatchLogsClient, PutLogEventsCommand, CreateLogGroupCommand, CreateLogStreamCommand, DescribeLogGroupsCommand } from "@aws-sdk/client-cloudwatch-logs";

let logsClient: CloudWatchLogsClient | null = null;
let sequenceToken: string | null = null;

function getLogsClient(): CloudWatchLogsClient {
  if (!logsClient) {
    const region = process.env.AWS_REGION || "us-east-1";
    const endpoint = process.env.AWS_ENDPOINT;
    logsClient = new CloudWatchLogsClient({
      region,
      ...(endpoint ? { endpoint } : {}),
    });
  }
  return logsClient;
}

export async function ensureLogGroup(logGroupName: string): Promise<void> {
  try {
    const client = getLogsClient();
    await client.send(new CreateLogGroupCommand({ logGroupName }));
  } catch (err: any) {
    if (err.name !== "ResourceAlreadyExistsException") {
      throw err;
    }
  }
}

export async function ensureLogStream(logGroupName: string, logStreamName: string): Promise<void> {
  try {
    const client = getLogsClient();
    await client.send(new CreateLogStreamCommand({ logGroupName, logStreamName }));
  } catch (err: any) {
    if (err.name !== "ResourceAlreadyExistsException") {
      throw err;
    }
  }
}

export async function sendToCloudWatch(
  logGroupName: string,
  logStreamName: string,
  message: string,
  timestamp?: Date
): Promise<void> {
  const client = getLogsClient();
  
  await ensureLogGroup(logGroupName);
  await ensureLogStream(logGroupName, logStreamName);
  
  const params: any = {
    logGroupName,
    logStreamName,
    logEvents: [
      {
        message,
        timestamp: timestamp ? timestamp.getTime() : Date.now(),
      },
    ],
  };
  
  if (sequenceToken) {
    params.sequenceToken = sequenceToken;
  }
  
  const response = await client.send(new PutLogEventsCommand(params));
  
  if (response.nextSequenceToken) {
    sequenceToken = response.nextSequenceToken;
  }
}

export async function sendJsonToCloudWatch(
  logGroupName: string,
  logStreamName: string,
  data: Record<string, any>
): Promise<void> {
  await sendToCloudWatch(logGroupName, logStreamName, JSON.stringify(data));
}
