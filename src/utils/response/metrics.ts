export class Metrics {
  private counters: Map<string, number> = new Map();
  private gauges: Map<string, number> = new Map();
  private histograms: Map<string, number[]> = new Map();

  increment(name: string, value = 1, tags: Record<string, string> = {}): void {
    const key = this.key(name, tags);
    this.counters.set(key, (this.counters.get(key) || 0) + value);
  }

  set(name: string, value: number, tags: Record<string, string> = {}): void {
    const key = this.key(name, tags);
    this.gauges.set(key, value);
  }

  observe(name: string, value: number, tags: Record<string, string> = {}): void {
    const key = this.key(name, tags);
    if (!this.histograms.has(key)) {
      this.histograms.set(key, []);
    }
    this.histograms.get(key)!.push(value);
  }

  private key(name: string, tags: Record<string, string>): string {
    const tagStr = Object.entries(tags)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}=${v}`)
      .join(",");
    return tagStr ? `${name},${tagStr}` : name;
  }

  getCounter(name: string, tags: Record<string, string> = {}): number {
    return this.counters.get(this.key(name, tags)) || 0;
  }

  getGauge(name: string, tags: Record<string, string> = {}): number | undefined {
    return this.gauges.get(this.key(name, tags));
  }

  getHistogram(name: string, tags: Record<string, string> = {}): number[] {
    return this.histograms.get(this.key(name, tags)) || [];
  }

  reset(): void {
    this.counters.clear();
    this.gauges.clear();
    this.histograms.clear();
  }

  toJSON(): Record<string, any> {
    return {
      counters: Object.fromEntries(this.counters),
      gauges: Object.fromEntries(this.gauges),
      histograms: Object.fromEntries(
        Array.from(this.histograms.entries()).map(([k, v]) => [
          k,
          { count: v.length, min: Math.min(...v), max: Math.max(...v), avg: v.reduce((a, b) => a + b, 0) / v.length },
        ])
      ),
    };
  }
}

export const metrics = new Metrics();
