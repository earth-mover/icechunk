/**
 * Browser HTTP transport for Repository.setHttpVirtualChunkFetcher().
 * The repository still requires explicit authorization for each URL prefix.
 */
export function createHttpVirtualChunkFetcher(fetchImpl = globalThis.fetch) {
  return async (_err, { url, rangeStart, rangeEnd, headers: configuredHeaders, options }) => {
    if (!Number.isSafeInteger(rangeStart) || !Number.isSafeInteger(rangeEnd) || rangeStart < 0 || rangeEnd <= rangeStart) {
      throw new Error('Invalid virtual chunk byte range')
    }
    const parsed = new URL(url)
    if (!['http:', 'https:'].includes(parsed.protocol) || parsed.username || parsed.password) {
      throw new Error('Virtual chunk fetch requires an HTTP(S) URL without embedded credentials')
    }
    if (Object.keys(options ?? {}).length) {
      throw new Error('Native HTTP transport options are not supported by browser fetch; configure a custom callback')
    }
    const headers = new Headers(configuredHeaders)
    headers.set('Range', `bytes=${rangeStart}-${rangeEnd - 1}`)
    // The Rust resolver validates checksums against metadata from this same
    // response. Avoid conditional headers that would force a CORS preflight.
    // Do not send ambient cookies or follow redirects beyond the authorized prefix.
    const response = await fetchImpl(url, { headers, credentials: 'omit', redirect: 'error' })
    if (response.status !== 206) {
      await response.body?.cancel()
      throw new Error(`Virtual chunk read requires HTTP 206, received ${response.status}`)
    }
    const contentRange = response.headers.get('Content-Range')
    const match = /^bytes (\d+)-(\d+)\/(\d+|\*)$/.exec(contentRange ?? '')
    if (!match || Number(match[1]) !== rangeStart || Number(match[2]) !== rangeEnd - 1) {
      await response.body?.cancel()
      throw new Error('Incorrect or missing Content-Range; cross-origin servers must expose this header via CORS')
    }
    const data = new Uint8Array(await response.arrayBuffer())
    if (data.byteLength !== rangeEnd - rangeStart) throw new Error('Incorrect virtual chunk response length')
    const modified = response.headers.get('Last-Modified')
    const timestamp = modified == null ? undefined : Math.floor(Date.parse(modified) / 1000)
    return {
      data,
      etag: response.headers.get('ETag') ?? undefined,
      lastModified: Number.isInteger(timestamp) && timestamp >= 0 && timestamp <= 0xffffffff ? timestamp : undefined,
    }
  }
}
