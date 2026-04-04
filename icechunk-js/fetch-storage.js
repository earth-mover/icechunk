/**
 * Read-only fetch-based storage backend for Icechunk.
 *
 * Usage:
 *   import { Repository } from '@earthmover/icechunk'
 *   import { createFetchStorage } from '@earthmover/icechunk/fetch-storage'
 *
 *   const storage = createFetchStorage('https://my-bucket.s3.amazonaws.com/my-repo.icechunk')
 *   const repo = await Repository.open(storage)
 */

const { Storage } = require('@earthmover/icechunk')

/**
 * @param {string} baseUrl - Base URL of the icechunk repository (no trailing slash)
 * @returns {Storage}
 */
function createFetchStorage(baseUrl) {
  // Normalize: strip trailing slash
  const base = baseUrl.replace(/\/+$/, '')

  function throwForStatus(resp, url) {
    if (resp.ok) return
    if (resp.status === 404) {
      throw new Error(`ObjectNotFound: ${url}`)
    }
    throw new Error(`HTTP ${resp.status}: ${url}`)
  }

  return Storage.newCustom({
    canWrite: async (_err) => false,

    getObjectRange: async (_err, { path, rangeStart, rangeEnd }) => {
      const url = `${base}/${path}`
      const headers = {}

      if (rangeStart != null && rangeEnd != null) {
        headers['Range'] = `bytes=${rangeStart}-${rangeEnd - 1}`
      } else if (rangeStart != null) {
        headers['Range'] = `bytes=${rangeStart}-`
      }

      const resp = await fetch(url, { headers })
      throwForStatus(resp, url)

      const data = new Uint8Array(await resp.arrayBuffer())
      const etag = resp.headers.get('etag') ?? undefined
      return { data, version: { etag } }
    },

    putObject: async () => {
      throw new Error('Read-only storage: putObject not supported')
    },

    copyObject: async () => {
      throw new Error('Read-only storage: copyObject not supported')
    },

    listObjects: async (_err, prefix) => {
      // Try S3-style XML listing
      // Derive bucket URL and key prefix from the base URL
      // e.g. https://bucket.s3.region.amazonaws.com/prefix -> bucket URL + key prefix
      const url = new URL(base)
      const keyPrefix = url.pathname.replace(/^\//, '') + '/' + prefix
      const listUrl = `${url.origin}/?list-type=2&prefix=${encodeURIComponent(keyPrefix)}`

      const resp = await fetch(listUrl)
      throwForStatus(resp, listUrl)

      const xml = await resp.text()
      const results = []
      const contentRegex = /<Contents>([\s\S]*?)<\/Contents>/g
      let match
      while ((match = contentRegex.exec(xml)) !== null) {
        const block = match[1]
        const key = block.match(/<Key>(.*?)<\/Key>/)?.[1]
        const lastModified = block.match(/<LastModified>(.*?)<\/LastModified>/)?.[1]
        const size = block.match(/<Size>(.*?)<\/Size>/)?.[1]
        if (key && lastModified && size) {
          const id = key.startsWith(keyPrefix) ? key.slice(keyPrefix.length) : key
          results.push({
            id,
            createdAt: new Date(lastModified),
            sizeBytes: Number(size),
          })
        }
      }

      return results
    },

    deleteBatch: async () => {
      throw new Error('Read-only storage: deleteBatch not supported')
    },

    getObjectLastModified: async (_err, path) => {
      const url = `${base}/${path}`
      const resp = await fetch(url, { method: 'HEAD' })
      throwForStatus(resp, url)
      const lastModified = resp.headers.get('last-modified')
      if (!lastModified) {
        throw new Error(`No Last-Modified header: ${url}`)
      }
      return new Date(lastModified)
    },

    getObjectConditional: async (_err, { path, previousVersion }) => {
      const url = `${base}/${path}`
      const headers = {}

      if (previousVersion?.etag) {
        headers['If-None-Match'] = previousVersion.etag
      }

      const resp = await fetch(url, { headers })

      if (resp.status === 304) {
        return { kind: 'on_latest_version' }
      }

      throwForStatus(resp, url)

      const data = new Uint8Array(await resp.arrayBuffer())
      const etag = resp.headers.get('etag') ?? undefined
      return { kind: 'modified', data, newVersion: { etag } }
    },
  })
}

module.exports = { createFetchStorage }
