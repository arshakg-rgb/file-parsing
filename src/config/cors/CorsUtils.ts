import { Request, Response, NextFunction } from "express";

class CorsUtils 
{
  /**
   * Setup CORS middleware
   */
  static setupCors() 
{
    return (req: Request, res: Response, next: NextFunction): void => 
{
      const allowedOrigins = process.env.ALLOWED_ORIGINS 
        ? process.env.ALLOWED_ORIGINS.split(",") 
        : ["*"];

      const origin = req.headers.origin;
      
      if (allowedOrigins.includes("*") || (origin && allowedOrigins.includes(origin))) 
{
        res.setHeader("Access-Control-Allow-Origin", origin || "*");
        res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
        res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With");
        res.setHeader("Access-Control-Allow-Credentials", "true");
      }

      if (req.method === "OPTIONS") 
{
        res.sendStatus(200);
        return;
      }

      next();
    };
  }
}

export default CorsUtils;
