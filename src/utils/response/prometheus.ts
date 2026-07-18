import ServiceManager, { Enforce } from "../../config/ServiceManager.js";
import { InstantiationError } from "../../errors/InstantiationError.js";
import { metrics } from "./metrics.js";

class PrometheusService extends ServiceManager 
{
  protected static instance: PrometheusService;

  private constructor(enforce: () => void) 
{
    if (enforce !== Enforce) 
{
      throw new InstantiationError("Cannot instantiate PrometheusService directly. Use getInstance()");
    }
    super(enforce);
  }

  public static getInstance(): PrometheusService 
{
    if (!PrometheusService.instance) 
{
      PrometheusService.instance = new PrometheusService(Enforce);
    }
    return PrometheusService.instance;
  }

  public formatPrometheusMetrics(): string 
{
    const lines: string[] = [];
  
    for (const [key, value] of Object.entries(metrics.toJSON().counters)) 
{
      const [name, ...tags] = key.split(",");
      const tagStr = tags.map((t) => 
{
        const [k, v] = t.split("=");
        return `${k}="${v}"`;
      }).join(",");
      lines.push(`# TYPE ${name} counter`);
      lines.push(`${name}{${tagStr}} ${value}`);
    }
  
    for (const [key, value] of Object.entries(metrics.toJSON().gauges)) 
{
      const [name, ...tags] = key.split(",");
      const tagStr = tags.map((t) => 
{
        const [k, v] = t.split("=");
        return `${k}="${v}"`;
      }).join(",");
      lines.push(`# TYPE ${name} gauge`);
      lines.push(`${name}{${tagStr}} ${value}`);
    }
  
    for (const [key, value] of Object.entries(metrics.toJSON().histograms)) 
{
      const [name, ...tags] = key.split(",");
      const tagStr = tags.map((t) => 
{
        const [k, v] = t.split("=");
        return `${k}="${v}"`;
      }).join(",");
      const hist = value as { count: number; min: number; max: number; avg: number };
      lines.push(`# TYPE ${name} histogram`);
      lines.push(`${name}_count{${tagStr}} ${hist.count}`);
      lines.push(`${name}_sum{${tagStr}} ${hist.count * hist.avg}`);
      lines.push(`${name}_min{${tagStr}} ${hist.min}`);
      lines.push(`${name}_max{${tagStr}} ${hist.max}`);
    }
  
    return lines.join("\n");
  }
}


export default PrometheusService;

const prometheusService = PrometheusService.getInstance();

export function formatPrometheusMetrics(): string 
{
  return prometheusService.formatPrometheusMetrics();
}
