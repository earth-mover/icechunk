import test from 'node:test'
import assert from 'node:assert/strict'
import { createHttpVirtualChunkFetcher } from '../http-virtual-chunks.mjs'

const request = { url: 'https://example.com/tiles/a.tif', rangeStart: 4, rangeEnd: 8, headers: {}, options: {} }
const reply = (headers = {}, status = 206) => new Response(new Uint8Array([1, 2, 3, 4]), {
  status, headers: { 'Content-Range': 'bytes 4-7/100', ETag: '"v1"', ...headers },
})

test('fetches an exact range without preflight-only headers, ambient credentials, or redirects', async () => {
  const fetcher = createHttpVirtualChunkFetcher(async (url, options) => {
    assert.equal(url, request.url)
    assert.equal(options.headers.get('Range'), 'bytes=4-7')
    assert.equal(options.headers.get('If-Match'), null)
    assert.equal(options.headers.get('If-Unmodified-Since'), null)
    assert.equal(options.credentials, 'omit')
    assert.equal(options.redirect, 'error')
    return reply({ 'Last-Modified': new Date(1000 * 1000).toUTCString() })
  })
  const result = await fetcher(null, { ...request, etag: 'v1', lastModified: 1000 })
  assert.deepEqual([...result.data], [1, 2, 3, 4])
  assert.equal(result.etag, '"v1"')
  assert.equal(result.lastModified, 1000)
})

for (const status of [200, 404, 412, 416]) {
  test(`rejects HTTP ${status}`, async () => {
    await assert.rejects(createHttpVirtualChunkFetcher(async () => reply({}, status))(null, request), /requires HTTP 206/)
  })
}
for (const contentRange of ['', 'bytes 0-3/100', 'bytes 4-8/100']) {
  test(`rejects invalid Content-Range ${contentRange}`, async () => {
    await assert.rejects(createHttpVirtualChunkFetcher(async () => reply({ 'Content-Range': contentRange }))(null, request), /Content-Range/)
  })
}
test('rejects truncated bodies and unsafe offsets', async () => {
  await assert.rejects(createHttpVirtualChunkFetcher(async () => new Response(new Uint8Array([1]), {
    status: 206, headers: { 'Content-Range': 'bytes 4-7/100' },
  }))(null, request), /response length/)
  await assert.rejects(createHttpVirtualChunkFetcher()(null, { ...request, rangeEnd: 2 ** 53 }), /byte range/)
})
test('does not silently ignore native transport options', async () => {
  await assert.rejects(createHttpVirtualChunkFetcher()(null, { ...request, options: { timeout: '1s' } }), /transport options/)
})
