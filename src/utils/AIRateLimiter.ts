/**
 * AIRateLimiter - Rate limiter for AI API calls
 * Implements token bucket algorithm with RPM and burst limits
 */
export class AIRateLimiter {
  private requests: number[] = [];
  private rpm: number;
  private burst: number;

  constructor(rpm: number, burst: number) {
    this.rpm = rpm;
    this.burst = burst;
  }

  async acquire(): Promise<void> {
    const now = Date.now();
    const oneMinuteAgo = now - 60000;
    
    this.requests = this.requests.filter(time => time > oneMinuteAgo);
    
    if (this.requests.length >= this.burst) {
      const oldestRequest = this.requests[0];
      const waitTime = oldestRequest + 60000 - now;
      if (waitTime > 0) {
        console.warn("ai_rate_limit_burst", { waitTime, currentRequests: this.requests.length, burst: this.burst });
        await new Promise(resolve => setTimeout(resolve, waitTime));
        this.requests = this.requests.filter(time => time > oneMinuteAgo);
      }
    }
    
    if (this.requests.length >= this.rpm) {
      const oldestRequest = this.requests[0];
      const waitTime = oldestRequest + 60000 - now;
      if (waitTime > 0) {
        console.warn("ai_rate_limit_rpm", { waitTime, currentRequests: this.requests.length, rpm: this.rpm });
        await new Promise(resolve => setTimeout(resolve, waitTime));
        this.requests = this.requests.filter(time => time > oneMinuteAgo);
      }
    }
    
    this.requests.push(now);
    console.debug("ai_rate_limit_acquired", { currentRequests: this.requests.length, rpm: this.rpm, burst: this.burst });
  }
}
