/**
 * Read-only fetch-based storage backend for Icechunk.
 *
 * Usage:
 *   import { Repository } from '@earthmover/icechunk'
 *   import { createFetchStorage } from '@earthmover/icechunk/fetch-storage'
 *
 *   const storage = createFetchStorage('https://my-bucket.s3.amazonaws.com/my-repo.icechunk')
 *   const repo = await Repository.open(storage)
 *
 * With `@earthmover/icechunk/web`, use the `createFetchStorage` it exports.
 */

import { Storage } from '@earthmover/icechunk'

import { fetchStorageBackend } from './fetch-storage-backend.mjs'

/**
 * @param {string} baseUrl - Base URL of the icechunk repository (no trailing slash)
 * @returns {Storage}
 */
export function createFetchStorage(baseUrl) {
  return Storage.newCustom(fetchStorageBackend(baseUrl))
}
