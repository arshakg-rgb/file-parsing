import { metrics } from "./metrics.js";

export function formatPrometheusMetrics(): string {
  const lines: string[] = [];
  
  // Format counters
  for (const [key, value] of Object.entries(metrics.toJSON().counters)) {
    const [name, ...tags] = key.split(",");
    const tagStr = tags.map((t) => {
      const [k, v] = t.split("=");
      return `${k}="${v}"`;
    }).join(",");
    lines.push(`# TYPE ${name} counter`);
    lines.push(`${name}{${tagStr}} ${value}`);
  }
  
  // Format gauges
  for (const [key, value] of Object.entries(metrics.toJSON().gauges)) {
    const [name, ...tags] = key.split(",");
    const tagStr = tags.map((t) => {
      const [k, v] = t.split("=");
      return `${k}="${v}"`;
    }).join(",");
    lines.push(`# TYPE ${name} gauge`);
    lines.push(`${name}{${tagStr}} ${value}`);
  }
  
  // Format histograms
  for (const [key, value] of Object.entries(metrics.toJSON().histograms)) {
    const [name, ...tags] = key.split(",");
    const tagStr = tags.map((t) => {
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
