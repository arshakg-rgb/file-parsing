/**
 * Metrics is responsible for metrics operations.
 */
export class Metrics {
    /**
   * Counters
   * @private
   */
  private counters: Map<string, number> = new Map();
    /**
   * Gauges
   * @private
   */
  private gauges: Map<string, number> = new Map();
    /**
   * Histograms
   * @private
   */
  private histograms: Map<string, number[]> = new Map();

    /**
   * Performs the increment operation.
   * @param name - The name value
   * @param value - The value to use
   * @param tags - The tags
   */
  increment(name: string, value = 1, tags: Record<string, string> = {}): void {
    const key = this.key(name, tags);
    this.counters.set(key, (this.counters.get(key) || 0) + value);
  }

    /**
   * Sets the operation
   * @param name - The name value
   * @param value - The value to use
   * @param tags - The tags
   */
  set(name: string, value: number, tags: Record<string, string> = {}): void {
    const key = this.key(name, tags);
    this.gauges.set(key, value);
  }

    /**
   * Observes the operation
   * @param name - The name value
   * @param value - The value to use
   * @param tags - The tags
   */
  observe(name: string, value: number, tags: Record<string, string> = {}): void {
    const key = this.key(name, tags);
    if (!this.histograms.has(key)) {
      this.histograms.set(key, []);
    }
    this.histograms.get(key)!.push(value);
  }

    /**
   * Performs the key operation.
   * @param name - The name value
   * @param tags - The tags
   * @returns The string result
   */
  private key(name: string, tags: Record<string, string>): string {
    const tagStr = Object.entries(tags)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}=${v}`)
      .join(",");
    return tagStr ? `${name},${tagStr}` : name;
  }

    /**
   * Gets counter
   * @param name - The name value
   * @param tags - The tags
   * @returns The numeric result
   */
  getCounter(name: string, tags: Record<string, string> = {}): number {
    return this.counters.get(this.key(name, tags)) || 0;
  }

    /**
   * Gets gauge
   * @param name - The name value
   * @param tags - The tags
   * @returns The number | undefined result
   */
  getGauge(name: string, tags: Record<string, string> = {}): number | undefined {
    return this.gauges.get(this.key(name, tags));
  }

    /**
   * Gets histogram
   * @param name - The name value
   * @param tags - The tags
   * @returns The list of results
   */
  getHistogram(name: string, tags: Record<string, string> = {}): number[] {
    return this.histograms.get(this.key(name, tags)) || [];
  }

    /**
   * Resets the operation
   */
  reset(): void {
    this.counters.clear();
    this.gauges.clear();
    this.histograms.clear();
  }

    /**
   * Performs the to j s o n operation.
   * @returns The record<string, unknown> result
   */
  toJSON(): Record<string, unknown> {
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

/**
 * The metrics
 */
export const metrics = new Metrics();
