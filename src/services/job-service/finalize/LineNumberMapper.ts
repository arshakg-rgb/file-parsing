/**
 * LineNumberMapper is responsible for line number mapper operations.
 */
export class LineNumberMapper
{
    /**
   * Computes line map
   * @param source - The source
   * @param offsets - The offsets
   * @returns The map<number, number> result
   */

  static computeLineMap(source: Buffer, offsets: number[]): Map<number, number>
  {
    const sortedOffsets: number[] = [...offsets].sort((a, b) => a - b);
    const lineMap = new Map<number, number>();
    let sourcePos: number = 0;
    let nextOffsetIndex: number = 0;
    let newlineCount: number = 0;

    while (sourcePos < source.length && nextOffsetIndex < sortedOffsets.length)
    {
      while (nextOffsetIndex < sortedOffsets.length && sortedOffsets[nextOffsetIndex] <= sourcePos)
      {
        lineMap.set(sortedOffsets[nextOffsetIndex], newlineCount + 1);
        nextOffsetIndex++;
      }

      if (source[sourcePos] === 0x0a)
      {
        newlineCount++;
      }
      sourcePos++;
    }

    while (nextOffsetIndex < sortedOffsets.length && sortedOffsets[nextOffsetIndex] <= sourcePos)
    {
      lineMap.set(sortedOffsets[nextOffsetIndex], newlineCount + 1);
      nextOffsetIndex++;
    }

    return lineMap;
  }
}
