import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";

/**
 * SecretsService is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class SecretsService extends ServiceManager {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: SecretsService;
    /**
   * Secrets Client
   * @private
   */
  private secretsClient: SecretsManagerClient | null = null;

    /**
   * Constructs a new SecretsService instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate SecretsService directly. Use getInstance()");
    }
    super(enforce);
  }

    /**
   * Gets the single instance of the SecretsService class.
   * @returns The single instance of the class
   */
  public static getInstance(): SecretsService {
    if (!SecretsService.instance) {
      SecretsService.instance = new SecretsService(Enforce);
    }
    return SecretsService.instance;
  }

    /**
   * Gets secrets client
   * @returns The secrets manager client result
   */
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

    /**
   * Gets secret
   * @param secretName - The secret name
   * @returns A promise that resolves to the result
   */
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

    /**
   * Gets secret json
   * @param secretName - The secret name
   * @returns A promise that resolves to the result
   */
  public async getSecretJson<T = Record<string, unknown>>(secretName: string): Promise<T | null> {
    const secret = await this.getSecret(secretName);
    if (!secret) return null;
  
    try {
      return JSON.parse(secret) as T;
    } catch (err) {
      throw new Error(`Failed to parse secret ${secretName} as JSON: ${err}`);
    }
  }

    /**
   * Loads all secrets
   */
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

/**
 * The secrets service
 */
const secretsService = SecretsService.getInstance();

/**
 * Gets secret
 * @param secretName - The secret name
 * @returns A promise that resolves to the result
 */
export async function getSecret(secretName: string): Promise<string | null> {
  return secretsService.getSecret(secretName);
}

/**
 * Gets secret json
 * @param secretName - The secret name
 * @returns A promise that resolves to the result
 */
export async function getSecretJson<T = Record<string, unknown>>(secretName: string): Promise<T | null> {
  return secretsService.getSecretJson<T>(secretName);
}

/**
 * Loads all secrets
 */
export async function loadAllSecrets(): Promise<void> {
  return secretsService.loadAllSecrets();
}
