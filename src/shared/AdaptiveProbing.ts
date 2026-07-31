import ServiceManager from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";


/**
 * AdaptiveProbing is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */

export class AdaptiveProbing extends ServiceManager
{
    /**
   * Singleton instance
   * @private
   */

  protected static instance: AdaptiveProbing;

    /**
   * P R O B E_ S I Z E_ P E R_ C O U N T
   * @private
   */

  private readonly PROBE_SIZE_PER_COUNT: number = 536870912;
    /**
   * P R O B E_ C O U N T_ M I N
   * @private
   */

  private readonly PROBE_COUNT_MIN: number = 1;

    /**
   * P R O B E_ C O U N T_ M A X
   * @private
   */

  private readonly PROBE_COUNT_MAX: number = 10;


    /**
   * Constructs a new AdaptiveProbing instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */

  private constructor(enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE,"Cannot instantiate AdaptiveProbing directly. Use getInstance()");
    }

    super(enforce);
  }

    /**
   * Gets the single instance of the AdaptiveProbing class.
   * @returns The single instance of the class
   */
  public static getInstance(): AdaptiveProbing
  {
    if (!AdaptiveProbing.instance)
    {
      AdaptiveProbing.instance = new AdaptiveProbing(Enforce);
    }

    return AdaptiveProbing.instance;
  }

    /**
   * Calculates probe count
   * @param fileSize - The file size
   * @returns The numeric result
   */

  public calculateProbeCount(fileSize: number): number
  {
    const sizePerProbe: number = this.PROBE_SIZE_PER_COUNT;
    const idealCount: number = Math.ceil(fileSize / sizePerProbe);

    return Math.max(this.PROBE_COUNT_MIN, Math.min(idealCount, this.PROBE_COUNT_MAX));
  }

    /**
   * Performs the generate probe offsets operation.
   * @param fileSize - The file size
   * @param probeCount - The probe count
   * @returns The list of results
   */

  public generateProbeOffsets(fileSize: number, probeCount: number): number[]
  {
    const offsets: number[] = [];

    if (probeCount === 1)
    {
      return [0];
    }

    const step: number = Math.floor(fileSize / probeCount);

    for (let i = 0; i < probeCount; i++)
    {
      offsets.push(i * step);
    }

    if (!offsets.includes(0))
    {
      offsets.push(0);
    }

    if (!offsets.includes(fileSize - 1))
    {
      offsets.push(fileSize - 1);
    }

    return [...new Set(offsets)].sort((a, b) => a - b);
  }

}


function Enforce(): void {}
