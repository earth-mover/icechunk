import type { HttpVirtualChunkRequest, HttpVirtualChunkResponse } from '@earthmover/icechunk'

/** Requires HTTP range support and CORS exposure of Content-Range and checksum headers. */
export declare function createHttpVirtualChunkFetcher(fetchImpl?: typeof fetch):
  (err: null, request: HttpVirtualChunkRequest) => Promise<HttpVirtualChunkResponse>
