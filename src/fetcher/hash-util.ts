import { createHash } from "crypto";

/**
 * Create checksum in SHA-256.
 * @param  {any}    data data
 * @return {string}      SHA-256 checksum
 */
export function sha256(data: string): string {
  const hash = createHash("sha256");
  hash.update(data);

  return hash.digest("base64");
}
