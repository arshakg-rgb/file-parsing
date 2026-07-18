import { IJobService, JobEvent } from "../io/IJobService.js";
import { InstantiationError } from "../../../errors/InstantiationError.js";
import { receiveMessages, deleteMessage } from "../../../shared/queueUtils.js";
import Config from "../../../config/system-config/Config.js";

function Enforce(): void 
{}

const config = Config.getInstance();

export class JobServiceImpl implements IJobService 
{
  private static instance: JobServiceImpl;
  private isRunning: boolean = false;

  private constructor(enforce: () => void) 
{
    if (enforce !== Enforce) 
{
      throw new InstantiationError("Cannot instantiate JobServiceImpl directly. Use getInstance()");
    }
  }

  public static getInstance(): JobServiceImpl 
{
    if (!JobServiceImpl.instance) 
{
      JobServiceImpl.instance = new JobServiceImpl(Enforce);
    }
    return JobServiceImpl.instance;
  }

  public async initialize(): Promise<void> 
{
    console.log("Initializing JobServiceImpl...");
  }

  public async startEventConsumer(): Promise<void> 
{
    if (this.isRunning) 
{
      console.warn("Event consumer is already running");
      return;
    }

    this.isRunning = true;
    console.log("Starting event consumer loop...");
    await this.eventConsumerLoop();
  }

  private async eventConsumerLoop(): Promise<void> 
{
    while (this.isRunning) 
{
      try 
{
        const messages = await receiveMessages<JobEvent>(
          config.settings.JOB_EVENTS_QUEUE_URL,
          (body) => JSON.parse(body) as JobEvent,
          10,
          5
        );
        
        for (const { payload, receiptHandle } of messages) 
{
          try 
{
            await this.handleEvent(payload);
            await deleteMessage(config.settings.JOB_EVENTS_QUEUE_URL, receiptHandle);
          }
 catch (exc) 
{
            const errorStr = String(exc);
            console.error("event_processing_error", { error: errorStr, body: payload });
          }
        }
      }
 catch (exc) 
{
        console.error("event_consumer_loop_error", { error: String(exc) });
        await new Promise((r) => setTimeout(r, 5000));
      }
    }
  }

  public async handleEvent(event: JobEvent): Promise<void> 
{
    console.log("Handling event:", event);
  }

  public async shutdown(): Promise<void> 
{
    console.log("Shutting down JobServiceImpl...");
    this.isRunning = false;
  }
}

