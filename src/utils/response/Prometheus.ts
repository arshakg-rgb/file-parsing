import { MetricsUtils } from "@utils/response/Metrics";

/**
 * PrometheusService is a static utility class responsible for formatting
 * metrics in Prometheus exposition format. All members are static; the
 * class is never instantiated.
 */

class PrometheusService
{
  /**
   * Formats prometheus metrics
   * @returns The string result
   */

  public static formatPrometheusMetrics(): string
  {
    const lines: string[] = [];
    const snapshot = MetricsUtils.toJSON() as {
      counters: Record<string, number>;
      gauges: Record<string, number>;
      histograms: Record<string, { count: number; min: number; max: number; avg: number }>;
    };

    for (const [key, value] of Object.entries(snapshot.counters))
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

    for (const [key, value] of Object.entries(snapshot.gauges))
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

    for (const [key, value] of Object.entries(snapshot.histograms))
    {
      const [name, ...tags] = key.split(",");
      const tagStr: string = tags.map((t) => {
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
