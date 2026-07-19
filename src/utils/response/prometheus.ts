import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { metrics } from "./metrics.js";

/**
 * PrometheusService is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class PrometheusService extends ServiceManager {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: PrometheusService;

    /**
   * Constructs a new PrometheusService instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate PrometheusService directly. Use getInstance()");
    }
    super(enforce);
  }

    /**
   * Gets the single instance of the PrometheusService class.
   * @returns The single instance of the class
   */
  public static getInstance(): PrometheusService {
    if (!PrometheusService.instance) {
      PrometheusService.instance = new PrometheusService(Enforce);
    }
    return PrometheusService.instance;
  }

    /**
   * Formats prometheus metrics
   * @returns The string result
   */
  public formatPrometheusMetrics(): string {
    const lines: string[] = [];
    const snapshot = metrics.toJSON() as {
      counters: Record<string, number>;
      gauges: Record<string, number>;
      histograms: Record<string, { count: number; min: number; max: number; avg: number }>;
    };

    for (const [key, value] of Object.entries(snapshot.counters)) {
      const [name, ...tags] = key.split(",");
      const tagStr = tags.map((t) => {
        const [k, v] = t.split("=");
        return `${k}="${v}"`;
      }).join(",");
      lines.push(`# TYPE ${name} counter`);
      lines.push(`${name}{${tagStr}} ${value}`);
    }
  
    for (const [key, value] of Object.entries(snapshot.gauges)) {
      const [name, ...tags] = key.split(",");
      const tagStr = tags.map((t) => {
        const [k, v] = t.split("=");
        return `${k}="${v}"`;
      }).join(",");
      lines.push(`# TYPE ${name} gauge`);
      lines.push(`${name}{${tagStr}} ${value}`);
    }

    for (const [key, value] of Object.entries(snapshot.histograms)) {
      const [name, ...tags] = key.split(",");
      const tagStr = tags.map((t) => {
        const [k, v] = t.split("=");
        return `${k}="${v}"`;
      }).join(",");
      lines.push(`# TYPE ${name} histogram`);
      lines.push(`${name}_count{${tagStr}} ${value.count}`);
      lines.push(`${name}_sum{${tagStr}} ${value.count * value.avg}`);
      lines.push(`${name}_min{${tagStr}} ${value.min}`);
      lines.push(`${name}_max{${tagStr}} ${value.max}`);
    }
  
    return lines.join("\n");
  }
}


export default PrometheusService;

/**
 * The prometheus service
 */
const prometheusService = PrometheusService.getInstance();

/**
 * Formats prometheus metrics
 * @returns The string result
 */
export function formatPrometheusMetrics(): string {
  return prometheusService.formatPrometheusMetrics();
}
