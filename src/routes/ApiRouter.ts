import { Router } from "express";

class ApiRouter 
{
  private static instance: ApiRouter;
  private router: Router;
  private versionedRoutes: Map<string, Router> = new Map();

  private constructor() 
{
    this.router = Router();
  }

  public static getInstance(): ApiRouter 
{
    if (!ApiRouter.instance) 
{
      ApiRouter.instance = new ApiRouter();
    }
    return ApiRouter.instance;
  }

  public getRouter(): Router 
{
    return this.router;
  }

  /**
   * Initialize versioned routes
   */
  public async initializeVersionedRoutes(): Promise<void> 
{
    console.log("Initializing versioned routes...");
    
    this.versionedRoutes.set("v1", Router());
    this.versionedRoutes.set("v2", Router());

    this.router.use("/v1", this.versionedRoutes.get("v1")!);
    this.router.use("/v2", this.versionedRoutes.get("v2")!);

    this.setupV1Routes();
    this.setupV2Routes();

    console.log("Versioned routes initialized");
  }

  private setupV1Routes(): void 
{
    const v1Router = this.versionedRoutes.get("v1")!;
    
    v1Router.get("/health", (req, res) => 
{
      res.json({ 
        status: "healthy", 
        version: "v1",
        timestamp: new Date().toISOString() 
      });
    });

  }

  private setupV2Routes(): void 
{
    const v2Router = this.versionedRoutes.get("v2")!;
    
    v2Router.get("/health", (req, res) => 
{
      res.json({ 
        status: "healthy", 
        version: "v2",
        timestamp: new Date().toISOString() 
      });
    });

  }

  /**
   * Get a specific version router
   */
  public getVersionRouter(version: string): Router | undefined 
{
    return this.versionedRoutes.get(version);
  }
}

export default ApiRouter;
