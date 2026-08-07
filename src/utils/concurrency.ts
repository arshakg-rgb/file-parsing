export type SettledResult<R> =
  | { status: "fulfilled"; value: R }
  | { status: "rejected"; reason: unknown };

/**
 * Apply an async function to every item in `items` with a bounded level of
 * concurrency. This avoids the memory/CPU spikes of an unbounded `Promise.all`
 * while still allowing work to proceed in parallel.
 *
 * Per-item failures are captured rather than tearing down the whole batch. The
 * caller decides whether partial failure is acceptable for its use case.
 *
 * @param items - The items to process.
 * @param concurrency - Maximum number of in-flight promises at once.
 * @param fn - The async work to perform for each item.
 * @returns A promise resolving to an array of settled results in input order.
 */
export async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T, index: number) => Promise<R>
): Promise<SettledResult<R>[]>
{
  const results: SettledResult<R>[] = new Array(items.length);
  let cursor = 0;

  async function worker(): Promise<void>
  {
    while (cursor < items.length)
    {
      const i = cursor++;
      try
      {
        const value = await fn(items[i], i);
        results[i] = { status: "fulfilled", value };
      }
      catch (reason)
      {
        results[i] = { status: "rejected", reason };
      }
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, worker)
  );
  return results;
}
