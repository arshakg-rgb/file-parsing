import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";

class SecretsService extends ServiceManager {
  protected static instance: SecretsService;
  private secretsClient: SecretsManagerClient | null = null;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate SecretsService directly. Use getInstance()");
    }
    super(enforce);
  }

  public static getInstance(): SecretsService {
    if (!SecretsService.instance) {
      SecretsService.instance = new SecretsService(Enforce);
    }
    return SecretsService.instance;
  }

  private getSecretsClient(): SecretsManagerClient {
    if (!this.secretsClient) {
      const region = process.env.AWS_REGION || "us-east-1";
      const endpoint = process.env.AWS_ENDPOINT;
      this.secretsClient = new SecretsManagerClient({
        region,
        ...(endpoint ? { endpoint } : {}),
      });
    }
    return this.secretsClient;
  }

  public async getSecret(secretName: string): Promise<string | null> {
    const envVar = secretName.toUpperCase().replace(/-/g, "_");
    if (process.env[envVar]) {
      return process.env[envVar];
    }

    try {
      const client = this.getSecretsClient();
      const response = await client.send(
        new GetSecretValueCommand({ SecretId: secretName })
      );
    
      if (response.SecretString) {
        return response.SecretString;
      }
    
      if (response.SecretBinary) {
        return Buffer.from(response.SecretBinary).toString("utf-8");
      }
    
      return null;
    } catch (err: unknown) {
      const e = err as { name?: string; message?: string };
      if (e.name === "ResourceNotFoundException") {
        return null;
      }
      throw new Error(`Failed to fetch secret ${secretName}: ${e.message}`);
    }
  }

  public async getSecretJson<T = Record<string, unknown>>(secretName: string): Promise<T | null> {
    const secret = await this.getSecret(secretName);
    if (!secret) return null;
  
    try {
      return JSON.parse(secret) as T;
    } catch (err) {
      throw new Error(`Failed to parse secret ${secretName} as JSON: ${err}`);
    }
  }

  public async loadAllSecrets(): Promise<void> {
    const secretMappings: Record<string, string> = {
      DATABASE_URL: "database-url",
      FIRESTORE_CREDENTIALS: "firestore-credentials",
      BEDROCK_API_KEY: "bedrock-api-key",
    };
  
    for (const [envKey, secretName] of Object.entries(secretMappings)) {
      if (!process.env[envKey]) {
        const secret = await this.getSecret(secretName);
        if (secret) {
          process.env[envKey] = secret;
        }
      }
    }
  }
}


export default SecretsService;

const secretsService = SecretsService.getInstance();

export async function getSecret(secretName: string): Promise<string | null> {
  return secretsService.getSecret(secretName);
}

export async function getSecretJson<T = Record<string, unknown>>(secretName: string): Promise<T | null> {
  return secretsService.getSecretJson<T>(secretName);
}

export async function loadAllSecrets(): Promise<void> {
  return secretsService.loadAllSecrets();
}
