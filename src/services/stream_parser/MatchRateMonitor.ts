import { settings } from "../../shared/Settings.js";

export class MatchRateMonitor {
  private window: string[];
  private floor: number;
  private capacity: number;

  constructor(window = settings.MATCH_RATE_WINDOW, floor = settings.MATCH_RATE_FLOOR) {
    this.window = [];
    this.floor = floor;
    this.capacity = window;
  }

  record(templateId: string, matched: boolean): void {
    this.window.push(matched ? "hit" : "miss");
    if (this.window.length > this.capacity) {
      this.window.shift();
    }
  }

  rate(): number {
    if (this.window.length === 0) return 0;
    return this.window.filter((x) => x === "hit").length / this.window.length;
  }

  checkWindow(): void {
    const r = this.rate();
    if (this.window.length >= this.capacity && r < this.floor) {
      throw new Error(`Match rate collapsed to ${r.toFixed(3)}; floor is ${this.floor}`);
    }
  }
}
