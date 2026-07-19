import { Express, Router } from "express";
import { InstantiationError } from "@errors/InstantiationError.js";

/**
 * ApiRouter is responsible for api router operations.
 */
class ApiRouter {
    /**
   * Singleton instance
   * @private
   */
  private static instance: ApiRouter;
    /**
   * Router
   * @private
   */
  private router: Router;
    /**
   * Versioned Routes
   * @private
   */
  private versionedRoutes: Map<string, Router> = new Map();

    /**
   * Constructs a new ApiRouter instance.
   */
  private constructor() {
    this.router = Router();
  }

    /**
   * Gets the single instance of the ApiRouter class.
   * @returns The single instance of the class
   */
  public static getInstance(): ApiRouter {
    if (!ApiRouter.instance) {
      ApiRouter.instance = new ApiRouter();
    }
    return ApiRouter.instance;
  }

    /**
   * Gets router
   * @returns The router result
   */
  public getRouter(): Router {
    return this.router;
  }

  /**
   * Initialize versioned routes
   */
  public async initializeVersionedRoutes(): Promise<void> {
    console.log("Initializing versioned routes...");
    
    // Create version-specific routers
    this.versionedRoutes.set("v1", Router());
    this.versionedRoutes.set("v2", Router());

    // Register version routes
    this.router.use("/v1", this.versionedRoutes.get("v1")!);
    this.router.use("/v2", this.versionedRoutes.get("v2")!);

    // Add routes to each version
    this.setupV1Routes();
    this.setupV2Routes();

    console.log("Versioned routes initialized");
  }

    /**
   * Sets up v1 routes
   */
  private setupV1Routes(): void {
    const v1Router = this.versionedRoutes.get("v1")!;
    
    // Health check
    v1Router.get("/health", (req, res) => {
      res.json({ 
        status: "healthy", 
        version: "v1",
        timestamp: new Date().toISOString() 
      });
    });

    // Add v1 specific routes here
    // v1Router.use("/jobService", jobServiceRoutes);
  }

    /**
   * Sets up v2 routes
   */
  private setupV2Routes(): void {
    const v2Router = this.versionedRoutes.get("v2")!;
    
    // Health check
    v2Router.get("/health", (req, res) => {
      res.json({ 
        status: "healthy", 
        version: "v2",
        timestamp: new Date().toISOString() 
      });
    });

    // Add v2 specific routes here
    // v2Router.use("/jobService", jobServiceRoutes);
  }

  /**
   * Get a specific version router
   */
  public getVersionRouter(version: string): Router | undefined {
    return this.versionedRoutes.get(version);
  }
}

export default ApiRouter;
