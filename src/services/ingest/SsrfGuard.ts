import { isIP } from "net";
import dns from "dns";
import { promisify } from "util";
import { settings } from "@shared/Settings.js";
import { SSRFError } from "@errors/SSRFError";
import {InstantiationError} from "@errors/InstantiationError";

/**
 * Guards outbound HTTP(S) fetches against SSRF: validates URLs/IPs against
 * private and reserved network ranges, and streams response bodies while
 * re-checking every redirect hop, enforcing a fetch timeout and a
 * max-download-size cap.
 *
 * Singleton — obtain the instance via SsrfGuard.getInstance(), never `new`.
 */

export class SsrfGuard
{
  private static instance: SsrfGuard;

  private static readonly BLOCKED_NETWORKS: Set<string> = new Set([
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "169.254.0.0/16",
    "127.0.0.0/8",
    "0.0.0.0/8",
    "100.64.0.0/10",
    "192.0.2.0/24",
    "198.51.100.0/24",
    "203.0.113.0/24",
  ]);

  private static readonly BLOCKED_V6: Set<string> = new Set(["::1/128", "fc00::/7", "fe80::/10", "::ffff:0:0/96"]);

  private readonly dnsLookup: (hostname: string) => Promise<{ address: string; family: number }>;

  private constructor(enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Error: Instantiation failed: Use SsrfGuard.getInstance() instead of new.");
    }

    this.dnsLookup = promisify(dns.lookup);
  }

  static getInstance(): SsrfGuard
  {
    if (!SsrfGuard.instance)
    {
      SsrfGuard.instance = new SsrfGuard(Enforce);
    }

    return SsrfGuard.instance;
  }

  /**
   * Checks whether blocked ip
   * @param ip - The ip
   * @returns True if the condition is met, false otherwise
   */

  private isBlockedIp(ip: string): boolean
  {
    if (ip.includes(":"))
    {
      for (const net of SsrfGuard.BLOCKED_V6)
      {
        if (this.isInNetwork(ip, net))
        {
          return true;
        }
      }
    }
    else
    {
      for (const net of SsrfGuard.BLOCKED_NETWORKS)
      {
        if (this.isInNetwork(ip, net))
        {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Checks whether in network
   * @param ip - The ip
   * @param network - The network
   * @returns True if the condition is met, false otherwise
   */

  private isInNetwork(ip: string, network: string): boolean
  {
    const [net, prefix] = network.split("/");
    const bits: number = parseInt(prefix, 10);

    if (ip.includes(":"))
    {
      return this.isInNetworkV6(ip, net, bits);
    }

    const ipNum: number = ip.split(".").reduce((acc, oct) => (acc << 8) | parseInt(oct, 10), 0);
    const netNum: number = net.split(".").reduce((acc, oct) => (acc << 8) | parseInt(oct, 10), 0);
    const mask: number = 0xffffffff << (32 - bits);
    return (ipNum & mask) === (netNum & mask);
  }

  /**
   * Expands i pv6
   * @param ip - The ip
   * @returns The number[] | null result
   */

  private expandIPv6(ip: string): number[] | null
  {
    let parts: string[] = ip.split(":");

    if (parts[parts.length - 1].includes("."))
    {
      const ipv4: string = parts.pop()!;
      const [a, b, c, d] = ipv4.split(".").map((n) => parseInt(n, 10));

      if ([a, b, c, d].some((n) => Number.isNaN(n) || n < 0 || n > 255))
      {
        return null;
      }

      parts.push(((a << 8) | b).toString(16));
      parts.push(((c << 8) | d).toString(16));
    }

    const index: number = parts.findIndex((p) => p === "");

    if (index !== -1)
    {
      const nonEmpty: string[] = parts.filter((p) => p !== "");
      const missing: number = 8 - nonEmpty.length;

      if (missing < 0)
      {
        return null;
      }

      parts = [
        ...nonEmpty.slice(0, index),
        ...Array(missing).fill("0"),
        ...nonEmpty.slice(index),
      ];
    }

    while (parts.length < 8) parts.push("0");

    const groups: number[] = parts.slice(0, 8).map((p) => parseInt(p || "0", 16));

    if (groups.some((n) => Number.isNaN(n) || n < 0 || n > 0xffff))
    {
      return null;
    }

    return groups;
  }

  /**
   * Checks whether in network v6
   * @param ip - The ip
   * @param net - The net
   * @param bits - The bits
   * @returns True if the condition is met, false otherwise
   */

  private isInNetworkV6(ip: string, net: string, bits: number): boolean
  {
    const ipGroups: number[] = this.expandIPv6(ip);
    const netGroups: number[] = this.expandIPv6(net);

    if (!ipGroups || !netGroups)
    {
      return false;
    }

    const ipBig: bigint = this.ipv6ToBigInt(ipGroups);
    const netBig: bigint = this.ipv6ToBigInt(netGroups);
    const mask: bigint = bits === 0 ? 0n : (1n << 128n) - (1n << (128n - BigInt(bits)));
    return (ipBig & mask) === (netBig & mask);
  }

  /**
   * Performs the ipv6 to big int operation.
   * @param groups - The groups
   * @returns The bigint result
   */

  private ipv6ToBigInt(groups: number[]): bigint
  {
    return groups.reduce((acc, g) => (acc << 16n) | BigInt(g), 0n);
  }

  /**
   * Checks url
   * @param url - The URL to process
   */

  async checkUrl(url: string): Promise<void>
  {
    const parsed = new URL(url);
    if (parsed.protocol === "gs:")
    {
      return;
    }

    if (parsed.protocol !== "http:" && parsed.protocol !== "https:")
    {
      throw new SSRFError(`Disallowed URL scheme: ${parsed.protocol}`);
    }

    if (parsed.username || parsed.password)
    {
      throw new SSRFError("URLs with embedded credentials are not allowed");
    }

    const hostname: string = parsed.hostname;

    if (!hostname)
    {
      throw new SSRFError("URL has no hostname");
    }

    if (isIP(hostname))
    {
      if (this.isBlockedIp(hostname))
      {
        throw new SSRFError(`Blocked: ${hostname} is in a private/reserved range`);
      }

      return;
    }

    try
    {
      const { address } = await this.dnsLookup(hostname);

      if (this.isBlockedIp(address))
      {
        throw new SSRFError(`Blocked: ${hostname} resolves to ${address} which is in a private/reserved range`);
      }
    }
    catch (err)
    {
      throw new SSRFError(`DNS resolution failed for ${hostname}: ${err}`);
    }
  }

  /**
   * Fetches url stream
   * @param url - The URL to process
   * @returns The async generator<buffer> result
   */
  async *fetchUrlStream(url: string): AsyncGenerator<Buffer>
  {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), settings.FETCH_TIMEOUT_SECONDS * 1000);

    try
    {
      let current: string = url;
      let redirects: number = 0;
      const maxRedirects = 5;

      while (true)
      {
        await this.checkUrl(current);
        const resp: Response = await fetch(current, {
          signal: controller.signal,
          headers: { "User-Agent": "file-parsing-pipeline/0.1" },
          redirect: "manual",
        });

        if (resp.status >= 300 && resp.status < 400)
        {
          if (redirects >= maxRedirects)
          {
            throw new SSRFError(`Too many redirects from ${url}`);
          }

          const location: string = resp.headers.get("location");

          if (!location)
          {
            throw new SSRFError("Redirect with no Location header");
          }

          current = new URL(location, current).href;
          redirects++;
          continue;
        }

        if (!resp.ok)
        {
          throw new SSRFError(`Fetch failed: ${resp.status} ${resp.statusText}`);
        }

        const contentLength: string = resp.headers.get("content-length");

        if (contentLength && parseInt(contentLength, 10) > settings.ALLOWED_FETCH_SIZE_BYTES)
        {
          throw new SSRFError(`Response Content-Length ${contentLength} exceeds limit ${settings.ALLOWED_FETCH_SIZE_BYTES}`);
        }

        if (!resp.body)
        {
          throw new SSRFError("No response body");
        }

        const reader = resp.body.getReader();
        let total: number = 0;

        while (true)
        {
          const { done, value } = await reader.read();

          if (done)
          {
            break;
          }

          total += value.length;

          if (total > settings.ALLOWED_FETCH_SIZE_BYTES)
          {
            throw new SSRFError(`Download exceeded size limit ${settings.ALLOWED_FETCH_SIZE_BYTES}`);
          }
          yield Buffer.from(value);
        }

        return;
      }
    }
    finally
    {
      clearTimeout(timeout);
    }
  }
}

/**
 * Function to enforce the Singleton pattern.
 */
function Enforce(): void
{
}
