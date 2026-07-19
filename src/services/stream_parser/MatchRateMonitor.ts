import { settings } from "@shared/Settings.js";

/**
 * MatchRateMonitor is responsible for match rate monitor operations.
 */
export class MatchRateMonitor {
    /**
   * Window
   * @private
   */
  private window: string[];
    /**
   * Floor
   * @private
   */
  private floor: number;
    /**
   * Capacity
   * @private
   */
  private capacity: number;

    /**
   * Constructs a new MatchRateMonitor instance.
   * @param window - The window
   * @param floor - The floor
   */
  constructor(window = settings.MATCH_RATE_WINDOW, floor = settings.MATCH_RATE_FLOOR) {
    this.window = [];
    this.floor = floor;
    this.capacity = window;
  }

    /**
   * Records the operation
   * @param templateId - The template id
   * @param matched - The matched
   */
  record(templateId: string, matched: boolean): void {
    this.window.push(matched ? "hit" : "miss");
    if (this.window.length > this.capacity) {
      this.window.shift();
    }
  }

    /**
   * Performs the rate operation.
   * @returns The numeric result
   */
  rate(): number {
    if (this.window.length === 0) return 0;
    return this.window.filter((x) => x === "hit").length / this.window.length;
  }

    /**
   * Checks window
   */
  checkWindow(): void {
    const r = this.rate();
    if (this.window.length >= this.capacity && r < this.floor) {
      throw new Error(`Match rate collapsed to ${r.toFixed(3)}; floor is ${this.floor}`);
    }
  }
}
