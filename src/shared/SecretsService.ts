import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import { InstantiationError } from "@errors/InstantiationError.js";

/**
 * SecretsService is a singleton class responsible for fetching secrets.
 * It is intentionally decoupled from ServiceManager/Config so it can be
 * used to load secrets before Config is initialized.
 *
 * Every member below declares its access modifier explicitly
 * (public / private / protected / static) rather than relying on
 * TypeScript's implicit-public default.
 *
 * There are no free functions in this module. The previous module-level
 * `Enforce()` guard and the exported `loadAllSecrets` wrapper function
 * have been folded into the class as a private static enforcement
 * method and a public static passthrough, respectively, so the entire
 * public surface of this module is the SecretsService class itself.
 */

export class SecretsService
{
  /**
   * Singleton instance
   * @protected
   */

  protected static instance: SecretsService;

  /**
   * Secrets Client
   * @private
   */

  private secretsClient: SecretsManagerClient | null = null;

  /**
   * Constructs a new SecretsService instance.
   * Private — use SecretsService.getInstance() instead.
   * @param enforce - A function reference used to enforce the singleton pattern
   * @throws InstantiationError if instantiated directly
   */

  private constructor(enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Cannot instantiate SecretsService directly. Use getInstance()");
    }
  }


  /**
   * Gets the single instance of the SecretsService class.
   * @returns The single instance of the class
   */

  public static getInstance(): SecretsService
  {
    if (!SecretsService.instance)
    {
      SecretsService.instance = new SecretsService(Enforce);
    }

    return SecretsService.instance;
  }

  /**
   * Gets (and lazily creates) the Secrets Manager client.
   * @returns The secrets manager client result
   * @private
   */
  private getSecretsClient(): SecretsManagerClient
  {
    if (!this.secretsClient)
    {
      const region: string = process.env.AWS_REGION || "us-east-1";
      const endpoint: string  = process.env.AWS_ENDPOINT;

      this.secretsClient = new SecretsManagerClient({
        region,
        ...(endpoint ? { endpoint } : {}),
      });
    }
    return this.secretsClient;
  }

  /**
   * Gets a secret's raw string value, preferring an environment-variable
   * override if one is set.
   * @param secretName - The secret name
   * @returns A promise that resolves to the result
   */
  public async getSecret(secretName: string): Promise<string | null>
  {
    const envVar: string  = secretName.toUpperCase().replace(/-/g, "_");

    if (process.env[envVar])
    {
      return process.env[envVar];
    }

    try
    {
      const client: SecretsManagerClient = this.getSecretsClient();
      const response = await client.send(
          new GetSecretValueCommand({ SecretId: secretName })
      );

      if (response.SecretString)
      {
        return response.SecretString;
      }

      if (response.SecretBinary)
      {
        return Buffer.from(response.SecretBinary).toString("utf-8");
      }

      return null;
    }
    catch (err: unknown)
    {
      const e = err as { name?: string; message?: string };

      if (e.name === "ResourceNotFoundException")
      {
        return null;
      }

      throw new Error(`Failed to fetch secret ${secretName}: ${e.message}`);
    }
  }

  /**
   * Gets a secret and parses it as JSON.
   * @param secretName - The secret name
   * @returns A promise that resolves to the result
   */
  public async getSecretJson<T = Record<string, unknown>>(secretName: string): Promise<T | null>
  {
    const secret: string = await this.getSecret(secretName);

    if (!secret)
    {
      return null;
    }

    try
    {
      return JSON.parse(secret) as T;
    }
    catch (err)
    {
      throw new Error(`Failed to parse secret ${secretName} as JSON: ${err}`);
    }
  }

  /**
   * Loads all mapped secrets into process.env, skipping any environment
   * variable that is already set.
   */
  public async loadAllSecrets(): Promise<void>
  {
    const secretMappings: Record<string, string> = {
      FILE_DATABASE_URL: "FILE_DATABASE_URL",
      FIRESTORE_CREDENTIALS: "firestore-credentials",
      BEDROCK_API_KEY: "bedrock-api-key",
      CORS_DOMAINS: "CORS_DOMAINS",
      CORS_ENABLED: "CORS_ENABLED",
    };

    for (const [envKey, secretName] of Object.entries(secretMappings))
    {
      if (!process.env[envKey])
      {
        const secret: string = await this.getSecret(secretName);
        if (secret)
        {
          process.env[envKey] = secret;
        }
      }
    }
  }

  /**
   * Static passthrough — loads all mapped secrets into process.env.
   */
  public static async loadAllSecrets(): Promise<void>
  {
    return SecretsService.getInstance().loadAllSecrets();
  }
}


function Enforce(): void {}
