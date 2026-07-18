import { isIP } from "net";
import dns from "dns";
import { promisify } from "util";
import { settings } from "../../shared/config.js";

const dnsLookup = promisify(dns.lookup);

const BLOCKED_NETWORKS = new Set([
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

const BLOCKED_V6 = new Set(["::1/128", "fc00::/7", "fe80::/10", "::ffff:0:0/96"]);

function isBlockedIp(ip: string): boolean 
{
  if (ip.includes(":")) 
{
    for (const net of BLOCKED_V6) 
{
      if (isInNetwork(ip, net)) return true;
    }
  }
 else 
{
    for (const net of BLOCKED_NETWORKS) 
{
      if (isInNetwork(ip, net)) return true;
    }
  }
  return false;
}

function isInNetwork(ip: string, network: string): boolean 
{
  const [net, prefix] = network.split("/");
  const bits = parseInt(prefix, 10);
  if (ip.includes(":")) 
{
    return isInNetworkV6(ip, net, bits);
  }
  const ipNum = ip.split(".").reduce((acc, oct) => (acc << 8) | parseInt(oct, 10), 0);
  const netNum = net.split(".").reduce((acc, oct) => (acc << 8) | parseInt(oct, 10), 0);
  const mask = 0xffffffff << (32 - bits);
  return (ipNum & mask) === (netNum & mask);
}

function expandIPv6(ip: string): number[] | null 
{
  let parts = ip.split(":");
  if (parts[parts.length - 1].includes(".")) 
{
    const ipv4 = parts.pop()!;
    const [a, b, c, d] = ipv4.split(".").map((n) => parseInt(n, 10));
    if ([a, b, c, d].some((n) => Number.isNaN(n) || n < 0 || n > 255)) return null;
    parts.push(((a << 8) | b).toString(16));
    parts.push(((c << 8) | d).toString(16));
  }
  const index = parts.findIndex((p) => p === "");
  if (index !== -1) 
{
    const nonEmpty = parts.filter((p) => p !== "");
    const missing = 8 - nonEmpty.length;
    if (missing < 0) return null;
    parts = [
      ...nonEmpty.slice(0, index),
      ...Array(missing).fill("0"),
      ...nonEmpty.slice(index),
    ];
  }
  while (parts.length < 8) parts.push("0");
  const groups = parts.slice(0, 8).map((p) => parseInt(p || "0", 16));
  if (groups.some((n) => Number.isNaN(n) || n < 0 || n > 0xffff)) return null;
  return groups;
}

function isInNetworkV6(ip: string, net: string, bits: number): boolean 
{
  const ipGroups = expandIPv6(ip);
  const netGroups = expandIPv6(net);
  if (!ipGroups || !netGroups) return false;
  const ipBig = ipv6ToBigInt(ipGroups);
  const netBig = ipv6ToBigInt(netGroups);
  const mask = bits === 0 ? 0n : (1n << 128n) - (1n << (128n - BigInt(bits)));
  return (ipBig & mask) === (netBig & mask);
}

function ipv6ToBigInt(groups: number[]): bigint 
{
  return groups.reduce((acc, g) => (acc << 16n) | BigInt(g), 0n);
}

export class SSRFError extends Error 
{
  constructor(message: string) 
{
    super(message);
    this.name = "SSRFError";
  }
}

export async function checkUrl(url: string): Promise<void> 
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
  const hostname = parsed.hostname;
  if (!hostname) throw new SSRFError("URL has no hostname");

  if (isIP(hostname)) 
{
    if (isBlockedIp(hostname)) 
{
      throw new SSRFError(`Blocked: ${hostname} is in a private/reserved range`);
    }
    return;
  }

  try 
{
    const { address } = await dnsLookup(hostname);
    if (isBlockedIp(address)) 
{
      throw new SSRFError(`Blocked: ${hostname} resolves to ${address} which is in a private/reserved range`);
    }
  }
 catch (err) 
{
    throw new SSRFError(`DNS resolution failed for ${hostname}: ${err}`);
  }
}

export async function* fetchUrlStream(url: string): AsyncGenerator<Buffer> 
{
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), settings.FETCH_TIMEOUT_SECONDS * 1000);
  try 
{
    let current = url;
    let redirects = 0;
    const maxRedirects = 5;

    while (true) 
{
      await checkUrl(current);
      const resp = await fetch(current, {
        signal: controller.signal,
        headers: { "User-Agent": "file-parsing-pipeline/0.1" },
        redirect: "manual",
      });

      if (resp.status >= 300 && resp.status < 400) 
{
        if (redirects >= maxRedirects) throw new SSRFError(`Too many redirects from ${url}`);
        const location = resp.headers.get("location");
        if (!location) throw new SSRFError("Redirect with no Location header");
        current = new URL(location, current).href;
        redirects++;
        continue;
      }

      if (!resp.ok) throw new SSRFError(`Fetch failed: ${resp.status} ${resp.statusText}`);
      const contentLength = resp.headers.get("content-length");
      if (contentLength && parseInt(contentLength, 10) > settings.ALLOWED_FETCH_SIZE_BYTES) 
{
        throw new SSRFError(`Response Content-Length ${contentLength} exceeds limit ${settings.ALLOWED_FETCH_SIZE_BYTES}`);
      }
      if (!resp.body) throw new SSRFError("No response body");
      const reader = resp.body.getReader();
      let total = 0;
      while (true) 
{
        const { done, value } = await reader.read();
        if (done) break;
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
