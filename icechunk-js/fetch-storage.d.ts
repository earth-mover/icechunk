import type { Storage } from '@earthmover/icechunk'

/**
 * Create a read-only storage backend that fetches objects over HTTP.
 *
 * Works with any publicly accessible icechunk repository hosted on S3-compatible
 * storage. Requires the bucket to support anonymous reads and S3 XML listing.
 *
 * @param baseUrl - Base URL of the icechunk repository (no trailing slash)
 *
 * @example
 * ```ts
 * import { Repository } from '@earthmover/icechunk'
 * import { createFetchStorage } from '@earthmover/icechunk/fetch-storage'
 *
 * const storage = createFetchStorage('https://my-bucket.s3.us-west-2.amazonaws.com/path/to/repo.icechunk')
 * const repo = await Repository.open(storage)
 * ```
 */
export declare function createFetchStorage(baseUrl: string): Storage
