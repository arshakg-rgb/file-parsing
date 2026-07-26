/**
 * Global constants for the application.
 */
export class Constants
{
  public static readonly ENVIRONMENTS: Record<string, string> = {
    PRODUCTION: "production",
    STAGING: "staging",
    DEVELOPMENT: "development"
  };

  public static readonly MAX_STRING_LENGTH: number = 255;
  public static readonly SIGINT: string = "SIGINT";
  public static readonly SIGTERM: string =  "SIGTERM";
}
