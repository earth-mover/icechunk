import test from 'ava'
// Import the ESM implementation while using its existing declaration file.
const { createHttpVirtualChunkFetcher }: typeof import('../http-virtual-chunks') = await import(
  new URL('../http-virtual-chunks.mjs', import.meta.url).href
)

const request = { url: 'https://example.com/tiles/a.tif', rangeStart: 4, rangeEnd: 8, headers: {}, options: {} }
const reply = (headers: Record<string, string> = {}, status = 206) =>
  new Response(new Uint8Array([1, 2, 3, 4]), {
    status,
    headers: { 'Content-Range': 'bytes 4-7/100', ETag: '"v1"', ...headers },
  })

test('fetches an exact range without preflight-only headers, ambient credentials, or redirects', async (t) => {
  const fetcher = createHttpVirtualChunkFetcher(async (url, options) => {
    t.is(url, request.url)
    const headers = new Headers(options?.headers)
    t.is(headers.get('Range'), 'bytes=4-7')
    t.is(headers.get('If-Match'), null)
    t.is(headers.get('If-Unmodified-Since'), null)
    t.is(options?.credentials, 'omit')
    t.is(options?.redirect, 'error')
    return reply({ 'Last-Modified': new Date(1000 * 1000).toUTCString() })
  })
  const result = await fetcher(null, { ...request, etag: 'v1', lastModified: 1000 })
  t.deepEqual([...result.data], [1, 2, 3, 4])
  t.is(result.etag, '"v1"')
  t.is(result.lastModified, 1000)
})

for (const status of [200, 404, 412, 416]) {
  test(`rejects HTTP ${status}`, async (t) => {
    await t.throwsAsync(createHttpVirtualChunkFetcher(async () => reply({}, status))(null, request), {
      message: /requires HTTP 206/,
    })
  })
}
for (const contentRange of ['', 'bytes 0-3/100', 'bytes 4-8/100']) {
  test(`rejects invalid Content-Range ${contentRange}`, async (t) => {
    await t.throwsAsync(
      createHttpVirtualChunkFetcher(async () => reply({ 'Content-Range': contentRange }))(null, request),
      { message: /Content-Range/ },
    )
  })
}
test('rejects truncated bodies and unsafe offsets', async (t) => {
  await t.throwsAsync(
    createHttpVirtualChunkFetcher(
      async () =>
        new Response(new Uint8Array([1]), {
          status: 206,
          headers: { 'Content-Range': 'bytes 4-7/100' },
        }),
    )(null, request),
    { message: /response length/ },
  )
  await t.throwsAsync(createHttpVirtualChunkFetcher()(null, { ...request, rangeEnd: 2 ** 53 }), {
    message: /byte range/,
  })
})
test('does not silently ignore native transport options', async (t) => {
  await t.throwsAsync(createHttpVirtualChunkFetcher()(null, { ...request, options: { timeout: '1s' } }), {
    message: /transport options/,
  })
})
