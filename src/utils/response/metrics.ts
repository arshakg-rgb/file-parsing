/**
 * MetricsUtils is a static utility class responsible for metrics operations.
 * All members are static; the class is never instantiated.
 */

export class MetricsUtils
{
  /**
   * Counters
   * @private
   */

  private static counters: Map<string, number> = new Map();

  /**
   * Gauges
   * @private
   */

  private static gauges: Map<string, number> = new Map();

  /**
   * Histograms
   * @private
   */

  private static histograms: Map<string, number[]> = new Map();

  /**
   * Performs the increment operation.
   * @param name - The name value
   * @param value - The value to use
   * @param tags - The tags
   */

  public static increment(name: string, value = 1, tags: Record<string, string> = {}): void
  {
    const key: string = MetricsUtils.key(name, tags);
    MetricsUtils.counters.set(key, (MetricsUtils.counters.get(key) || 0) + value);
  }

  /**
   * Sets the operation
   * @param name - The name value
   * @param value - The value to use
   * @param tags - The tags
   */

  public static set(name: string, value: number, tags: Record<string, string> = {}): void
  {
    const key: string = MetricsUtils.key(name, tags);
    MetricsUtils.gauges.set(key, value);
  }

  /**
   * Performs the key operation.
   * @param name - The name value
   * @param tags - The tags
   * @returns The string result
   */
  private static key(name: string, tags: Record<string, string>): string
  {
    const tagStr: string = Object.entries(tags)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => `${k}=${v}`)
        .join(",");

    return tagStr ? `${name},${tagStr}` : name;
  }

  /**
   * Performs the to j s o n operation.
   * @returns The record<string, unknown> result
   */
  public static toJSON(): Record<string, unknown>
  {
    return {
      counters: Object.fromEntries(MetricsUtils.counters),
      gauges: Object.fromEntries(MetricsUtils.gauges),
      histograms: Object.fromEntries(
          Array.from(MetricsUtils.histograms.entries()).map(([k, v]) => [
            k,
            { count: v.length, min: Math.min(...v), max: Math.max(...v), avg: v.reduce((a, b) => a + b, 0) / v.length },
          ])
      ),
    };
  }
}
