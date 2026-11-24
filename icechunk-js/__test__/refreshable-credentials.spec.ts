import test from 'ava'
import { createServer } from 'node:http'
import type { AddressInfo } from 'node:net'

import { Storage, Repository } from './binding'
import type { S3StaticCredentials } from '../index'

// The refreshable-credential factories are native-only (not in the WASI build).
const nativeTest = typeof Storage.newS3WithRefreshableCredentials === 'function' ? test : test.skip

// A minimal S3 stand-in: it answers every request with 404 and records the
// access key id from each SigV4 `Authorization` header.
async function withFakeS3(fn: (endpointUrl: string, accessKeys: string[]) => Promise<void>) {
  const accessKeys: string[] = []
  const server = createServer((req, res) => {
    const match = /Credential=([^/]+)\//.exec(req.headers.authorization ?? '')
    accessKeys.push(match?.[1] ?? '<unsigned>')
    res.writeHead(404, { 'Content-Type': 'application/xml' })
    res.end('<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>not found</Message></Error>')
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const { port } = server.address() as AddressInfo
  try {
    await fn(`http://127.0.0.1:${port}`, accessKeys)
  } finally {
    server.closeAllConnections()
    await new Promise((resolve) => server.close(resolve))
  }
}

function s3Storage(endpointUrl: string, callback: () => S3StaticCredentials | Promise<S3StaticCredentials>) {
  return Storage.newS3WithRefreshableCredentials('testbucket', 'refreshable', callback, undefined, {
    region: 'us-east-1',
    endpointUrl,
    allowHttp: true,
    forcePathStyle: true,
  })
}

const noRetries = { retries: { maxTries: 1 } }

nativeTest('refreshable S3 credentials from an async callback', async (t) => {
  await withFakeS3(async (endpointUrl, accessKeys) => {
    let calls = 0
    const storage = s3Storage(endpointUrl, async () => {
      calls += 1
      await new Promise((resolve) => setTimeout(resolve, 10))
      return { accessKeyId: 'async-key', secretAccessKey: 'secret', expiresAfter: new Date(Date.now() + 3600_000) }
    })
    t.false(await Repository.exists(storage, noRetries))
    t.false(await Repository.exists(storage, noRetries))
    // Credentials that expire in an hour are fetched once and cached.
    t.is(calls, 1)
    t.true(accessKeys.length > 0)
    t.true(accessKeys.every((key) => key === 'async-key'))
  })
})

nativeTest('refreshable S3 credentials from a sync callback', async (t) => {
  await withFakeS3(async (endpointUrl, accessKeys) => {
    let calls = 0
    const storage = s3Storage(endpointUrl, () => {
      calls += 1
      return { accessKeyId: 'sync-key', secretAccessKey: 'secret' }
    })
    t.false(await Repository.exists(storage, noRetries))
    t.is(calls, 1)
    t.true(accessKeys.length > 0)
    t.true(accessKeys.every((key) => key === 'sync-key'))
  })
})

nativeTest('refreshable S3 credentials are fetched again after they expire', async (t) => {
  await withFakeS3(async (endpointUrl, accessKeys) => {
    let calls = 0
    const storage = s3Storage(endpointUrl, async () => {
      calls += 1
      // Expires well inside the refresh window, so every request refreshes.
      return { accessKeyId: `key-${calls}`, secretAccessKey: 'secret', expiresAfter: new Date(Date.now() + 1000) }
    })
    t.false(await Repository.exists(storage, noRetries))
    const callsAfterFirst = calls
    t.false(await Repository.exists(storage, noRetries))
    t.true(calls > callsAfterFirst)
    t.true(accessKeys.includes('key-1'))
    t.true(accessKeys.includes(`key-${calls}`))
  })
})

nativeTest('a rejected credentials promise fails the request', async (t) => {
  await withFakeS3(async (endpointUrl, accessKeys) => {
    const storage = s3Storage(endpointUrl, async () => {
      throw new Error('no credentials for you')
    })
    await t.throwsAsync(Repository.exists(storage, noRetries))
    t.deepEqual(accessKeys, [])
  })
})
