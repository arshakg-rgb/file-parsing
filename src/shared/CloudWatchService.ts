import { CloudWatchLogsClient, PutLogEventsCommand, CreateLogGroupCommand, CreateLogStreamCommand, DescribeLogGroupsCommand, PutLogEventsCommandInput } from "@aws-sdk/client-cloudwatch-logs";
import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { createLogger, Logger } from "@utils/logger/logger.js";

/**
 * CloudWatchService is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class CloudWatchService extends ServiceManager 
{
  /**
   * Singleton instance
   * @private
   */

  protected static instance: CloudWatchService;
  
  /**
   * Logs Client
   * @private
   */

  private logsClient: CloudWatchLogsClient | null = null;

   /**
   * Sequence Tokens
   * @private
   */

  private sequenceTokens: Map<string, string> = new Map();

  /**
   * Logger instance
   * @private
   */

  private logger: Logger;

  /**
   * Constructs a new CloudWatchService instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */

  private constructor(enforce: () => void)
   {
    if (enforce !== Enforce) 
    {
      throw new InstantiationError("Cannot instantiate CloudWatchService directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("cloudwatch");
  }

    /**
   * Gets the single instance of the CloudWatchService class.
   * @returns The single instance of the class
   */
  public static getInstance(): CloudWatchService {
    if (!CloudWatchService.instance) {
      CloudWatchService.instance = new CloudWatchService(Enforce);
    }
    return CloudWatchService.instance;
  }

    /**
   * Gets logs client
   * @returns The cloud watch logs client result
   */
  private getLogsClient(): CloudWatchLogsClient {
    if (!this.logsClient) {
      const region = process.env.AWS_REGION || "us-east-1";
      const endpoint = process.env.AWS_ENDPOINT;
      this.logsClient = new CloudWatchLogsClient({
        region,
        ...(endpoint ? { endpoint } : {}),
      });
    }
    return this.logsClient;
  }

    /**
   * Gets sequence token key
   * @param logGroupName - The log group name
   * @param logStreamName - The log stream name
   * @returns The string result
   */
  private getSequenceTokenKey(logGroupName: string, logStreamName: string): string {
    return `${logGroupName}:${logStreamName}`;
  }

    /**
   * Ensures log group
   * @param logGroupName - The log group name
   */
  public async ensureLogGroup(logGroupName: string): Promise<void> {
    try {
      const client = this.getLogsClient();
      await client.send(new CreateLogGroupCommand({ logGroupName }));
      this.logger.debug("log_group_created", { logGroupName });
    } catch (err: unknown) {
      const e = err as { name?: string; message?: string };
      if (e.name !== "ResourceAlreadyExistsException") {
        this.logger.error("log_group_creation_failed", { logGroupName, error: e.message });
        throw err;
      }
      this.logger.debug("log_group_already_exists", { logGroupName });
    }
  }

    /**
   * Ensures log stream
   * @param logGroupName - The log group name
   * @param logStreamName - The log stream name
   */
  public async ensureLogStream(logGroupName: string, logStreamName: string): Promise<void> {
    try {
      const client = this.getLogsClient();
      await client.send(new CreateLogStreamCommand({ logGroupName, logStreamName }));
      this.logger.debug("log_stream_created", { logGroupName, logStreamName });
    } catch (err: unknown) {
      const e = err as { name?: string; message?: string };
      if (e.name !== "ResourceAlreadyExistsException") {
        this.logger.error("log_stream_creation_failed", { logGroupName, logStreamName, error: e.message });
        throw err;
      }
      this.logger.debug("log_stream_already_exists", { logGroupName, logStreamName });
    }
  }

    /**
   * Sends to cloud watch
   * @param logGroupName - The log group name
   * @param logStreamName - The log stream name
   * @param message - The message
   * @param timestamp - The timestamp
   */
  public async sendToCloudWatch(
    logGroupName: string,
    logStreamName: string,
    message: string,
    timestamp?: Date
  ): Promise<void> {
    const client = this.getLogsClient();
  
    await this.ensureLogGroup(logGroupName);
    await this.ensureLogStream(logGroupName, logStreamName);
  
    const tokenKey = this.getSequenceTokenKey(logGroupName, logStreamName);
    const sequenceToken = this.sequenceTokens.get(tokenKey);
  
    const params: PutLogEventsCommandInput = {
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
      this.sequenceTokens.set(tokenKey, response.nextSequenceToken);
    }
    
    this.logger.debug("log_event_sent", { logGroupName, logStreamName });
  }

    /**
   * Sends json to cloud watch
   * @param logGroupName - The log group name
   * @param logStreamName - The log stream name
   * @param data - The data to process
   */
  public async sendJsonToCloudWatch(
    logGroupName: string,
    logStreamName: string,
    data: Record<string, unknown>
  ): Promise<void> {
    await this.sendToCloudWatch(logGroupName, logStreamName, JSON.stringify(data));
  }

    /**
   * Checks log group exists
   * @param logGroupName - The log group name
   * @returns True if the operation succeeds, false otherwise
   */
  public async checkLogGroupExists(logGroupName: string): Promise<boolean> {
    try {
      const client = this.getLogsClient();
      const response = await client.send(new DescribeLogGroupsCommand({
        logGroupNamePrefix: logGroupName,
        limit: 1,
      }));
      return response.logGroups?.some(group => group.logGroupName === logGroupName) || false;
    } catch (err: unknown) {
      this.logger.error("log_group_check_failed", { logGroupName, error: (err as { message?: string }).message });
      return false;
    }
  }
}


export default CloudWatchService;

/**
 * The cloud watch service
 */
const cloudWatchService = CloudWatchService.getInstance();

/**
 * Ensures log group
 * @param logGroupName - The log group name
 */
export async function ensureLogGroup(logGroupName: string): Promise<void> {
  return cloudWatchService.ensureLogGroup(logGroupName);
}

/**
 * Ensures log stream
 * @param logGroupName - The log group name
 * @param logStreamName - The log stream name
 */
export async function ensureLogStream(logGroupName: string, logStreamName: string): Promise<void> {
  return cloudWatchService.ensureLogStream(logGroupName, logStreamName);
}

/**
 * Sends to cloud watch
 * @param logGroupName - The log group name
 * @param logStreamName - The log stream name
 * @param message - The message
 * @param timestamp - The timestamp
 */
export async function sendToCloudWatch(
  logGroupName: string,
  logStreamName: string,
  message: string,
  timestamp?: Date
): Promise<void> {
  return cloudWatchService.sendToCloudWatch(logGroupName, logStreamName, message, timestamp);
}

/**
 * Sends json to cloud watch
 * @param logGroupName - The log group name
 * @param logStreamName - The log stream name
 * @param data - The data to process
 */
export async function sendJsonToCloudWatch(
  logGroupName: string,
  logStreamName: string,
  data: Record<string, unknown>
): Promise<void> {
  return cloudWatchService.sendJsonToCloudWatch(logGroupName, logStreamName, data);
}
