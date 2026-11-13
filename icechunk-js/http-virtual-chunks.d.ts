import type { HttpVirtualChunkRequest, HttpVirtualChunkResponse } from '@earthmover/icechunk'

/**
 * Browser HTTP transport for Repository.setHttpVirtualChunkFetcher().
 * Requires HTTP 206 range responses. Cross-origin servers must allow CORS and
 * expose ETag when references record ETag checksums. Content-Range is checked
 * when exposed; otherwise the response must have exactly the requested length.
 */
export declare function createHttpVirtualChunkFetcher(fetchImpl?: typeof fetch):
  (err: null, request: HttpVirtualChunkRequest) => Promise<HttpVirtualChunkResponse>
