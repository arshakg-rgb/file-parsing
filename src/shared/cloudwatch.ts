import { CloudWatchLogsClient, PutLogEventsCommand, CreateLogGroupCommand, CreateLogStreamCommand, DescribeLogGroupsCommand } from "@aws-sdk/client-cloudwatch-logs";
import ServiceManager, { Enforce } from "../config/ServiceManager.js";
import { InstantiationError } from "../errors/InstantiationError.js";
import { createLogger } from "../utils/logger/logger.js";

class CloudWatchService extends ServiceManager 
{
  protected static instance: CloudWatchService;
  private logsClient: CloudWatchLogsClient | null = null;
  private sequenceTokens: Map<string, string> = new Map();
  private logger: any;

  private constructor(enforce: () => void) 
{
    if (enforce !== Enforce) 
{
      throw new InstantiationError("Cannot instantiate CloudWatchService directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("cloudwatch");
  }

  public static getInstance(): CloudWatchService 
{
    if (!CloudWatchService.instance) 
{
      CloudWatchService.instance = new CloudWatchService(Enforce);
    }
    return CloudWatchService.instance;
  }

  private getLogsClient(): CloudWatchLogsClient 
{
    if (!this.logsClient) 
{
      const region = process.env.AWS_REGION || "us-east-1";
      const endpoint = process.env.AWS_ENDPOINT;
      this.logsClient = new CloudWatchLogsClient({
        region,
        ...(endpoint ? { endpoint } : {}),
      });
    }
    return this.logsClient;
  }

  private getSequenceTokenKey(logGroupName: string, logStreamName: string): string 
{
    return `${logGroupName}:${logStreamName}`;
  }

  public async ensureLogGroup(logGroupName: string): Promise<void> 
{
    try 
{
      const client = this.getLogsClient();
      await client.send(new CreateLogGroupCommand({ logGroupName }));
      this.logger.debug("log_group_created", { logGroupName });
    }
 catch (err: any) 
{
      if (err.name !== "ResourceAlreadyExistsException") 
{
        this.logger.error("log_group_creation_failed", { logGroupName, error: err.message });
        throw err;
      }
      this.logger.debug("log_group_already_exists", { logGroupName });
    }
  }

  public async ensureLogStream(logGroupName: string, logStreamName: string): Promise<void> 
{
    try 
{
      const client = this.getLogsClient();
      await client.send(new CreateLogStreamCommand({ logGroupName, logStreamName }));
      this.logger.debug("log_stream_created", { logGroupName, logStreamName });
    }
 catch (err: any) 
{
      if (err.name !== "ResourceAlreadyExistsException") 
{
        this.logger.error("log_stream_creation_failed", { logGroupName, logStreamName, error: err.message });
        throw err;
      }
      this.logger.debug("log_stream_already_exists", { logGroupName, logStreamName });
    }
  }

  public async sendToCloudWatch(
    logGroupName: string,
    logStreamName: string,
    message: string,
    timestamp?: Date
  ): Promise<void> 
{
    const client = this.getLogsClient();
  
    await this.ensureLogGroup(logGroupName);
    await this.ensureLogStream(logGroupName, logStreamName);
  
    const tokenKey = this.getSequenceTokenKey(logGroupName, logStreamName);
    const sequenceToken = this.sequenceTokens.get(tokenKey);
  
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
  
    if (sequenceToken) 
{
      params.sequenceToken = sequenceToken;
    }
  
    const response = await client.send(new PutLogEventsCommand(params));
  
    if (response.nextSequenceToken) 
{
      this.sequenceTokens.set(tokenKey, response.nextSequenceToken);
    }
    
    this.logger.debug("log_event_sent", { logGroupName, logStreamName });
  }

  public async sendJsonToCloudWatch(
    logGroupName: string,
    logStreamName: string,
    data: Record<string, any>
  ): Promise<void> 
{
    await this.sendToCloudWatch(logGroupName, logStreamName, JSON.stringify(data));
  }

  public async checkLogGroupExists(logGroupName: string): Promise<boolean> 
{
    try 
{
      const client = this.getLogsClient();
      const response = await client.send(new DescribeLogGroupsCommand({
        logGroupNamePrefix: logGroupName,
        limit: 1,
      }));
      return response.logGroups?.some(group => group.logGroupName === logGroupName) || false;
    }
 catch (err: any) 
{
      this.logger.error("log_group_check_failed", { logGroupName, error: err.message });
      return false;
    }
  }
}


export default CloudWatchService;

const cloudWatchService = CloudWatchService.getInstance();

export async function ensureLogGroup(logGroupName: string): Promise<void> 
{
  return cloudWatchService.ensureLogGroup(logGroupName);
}

export async function ensureLogStream(logGroupName: string, logStreamName: string): Promise<void> 
{
  return cloudWatchService.ensureLogStream(logGroupName, logStreamName);
}

export async function sendToCloudWatch(
  logGroupName: string,
  logStreamName: string,
  message: string,
  timestamp?: Date
): Promise<void> 
{
  return cloudWatchService.sendToCloudWatch(logGroupName, logStreamName, message, timestamp);
}

export async function sendJsonToCloudWatch(
  logGroupName: string,
  logStreamName: string,
  data: Record<string, any>
): Promise<void> 
{
  return cloudWatchService.sendJsonToCloudWatch(logGroupName, logStreamName, data);
}
