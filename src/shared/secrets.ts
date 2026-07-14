import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";

let secretsClient: SecretsManagerClient | null = null;

function getSecretsClient(): SecretsManagerClient {
  if (!secretsClient) {
    const region = process.env.AWS_REGION || "us-east-1";
    const endpoint = process.env.AWS_ENDPOINT;
    secretsClient = new SecretsManagerClient({
      region,
      ...(endpoint ? { endpoint } : {}),
    });
  }
  return secretsClient;
}

export async function getSecret(secretName: string): Promise<string | null> {
  // First check environment variable (fallback)
  const envVar = secretName.toUpperCase().replace(/-/g, "_");
  if (process.env[envVar]) {
    return process.env[envVar];
  }

  // Try AWS Secrets Manager
  try {
    const client = getSecretsClient();
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
  } catch (err: any) {
    if (err.name === "ResourceNotFoundException") {
      return null;
    }
    throw new Error(`Failed to fetch secret ${secretName}: ${err.message}`);
  }
}

export async function getSecretJson<T = Record<string, any>>(secretName: string): Promise<T | null> {
  const secret = await getSecret(secretName);
  if (!secret) return null;
  
  try {
    return JSON.parse(secret) as T;
  } catch (err) {
    throw new Error(`Failed to parse secret ${secretName} as JSON: ${err}`);
  }
}

export async function loadAllSecrets(): Promise<void> {
  const secretMappings: Record<string, string> = {
    DATABASE_URL: "database-url",
    FIRESTORE_CREDENTIALS: "firestore-credentials",
    BEDROCK_API_KEY: "bedrock-api-key",
  };
  
  for (const [envKey, secretName] of Object.entries(secretMappings)) {
    if (!process.env[envKey]) {
      const secret = await getSecret(secretName);
      if (secret) {
        process.env[envKey] = secret;
      }
    }
  }
}
