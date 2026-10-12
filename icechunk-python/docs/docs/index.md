---
template: home.html
title: Icechunk - Open-source, high-performance array format and library for scientific data
---

<div class="benefit-row" markdown>
<div class="benefit-text" markdown>

## Scale and Speed {#scale}

Icechunk works the same whether you store gigabytes on a laptop, terabytes in a bucket, or reference petabytes of existing files from a repository of a few gigabytes.

Reads stay fast at every size: Icechunk fetches only the chunks you need and downloads them in parallel.

Individual researchers use Icechunk, and government agencies are adopting it for national-scale projects.
NOAA's National Weather Service has adopted it for its CIRRUS cloud data platform ([blog post](https://www.earthmover.io/blog/icechunk-at-nws-cirrus)), and a NASA feasibility report recommends it for virtual data stores ([report](https://nasa-impact.github.io/virtual-stores-feasibility-report/recommendations.html)).

[:octicons-arrow-right-24: Tuning performance](guides/performance.md) ·
[:octicons-arrow-right-24: Parallel writes](understanding/parallel.md)

</div>
<div class="benefit-figure">
<div class="fig scale" role="img" aria-label="Icechunk stores gigabytes on a laptop disk or terabytes in a bucket as native chunks, and references petabytes of existing files in place with a repository of only gigabytes.">
<div class="scale-col">
<svg class="fig-icon" viewBox="0 0 24 24"><path d="M6 2h12a2 2 0 0 1 2 2v16a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2m6 2a6 6 0 0 0-6 6c0 3.31 2.69 6 6.1 6l-.88-2.23a1.01 1.01 0 0 1 .37-1.37l.86-.5a1.01 1.01 0 0 1 1.37.37l1.92 2.42A5.98 5.98 0 0 0 18 10a6 6 0 0 0-6-6m0 5a1 1 0 0 1 1 1 1 1 0 0 1-1 1 1 1 0 0 1-1-1 1 1 0 0 1 1-1m-5 9a1 1 0 0 0-1 1 1 1 0 0 0 1 1 1 1 0 0 0 1-1 1 1 0 0 0-1-1m5.09-4.73 2.49 6.31 2.59-1.5-4.22-5.31z"/></svg>
<span class="fig-title">Gigabytes</span>
<span class="fig-sub">stored on your laptop's disk</span>
<svg class="scale-grid" viewBox="0 0 88 88" style="max-width: 88px"><defs><pattern id="scale-gb" width="22" height="22" patternUnits="userSpaceOnUse"><rect class="native" x="2" y="2" width="18" height="18" rx="3"/></pattern></defs><rect fill="url(#scale-gb)" width="88" height="88"/></svg>
<span class="scale-unit">GB</span>
</div>
<div class="scale-col">
<svg class="fig-icon" viewBox="0 0 24 24"><path d="M6.5 20q-2.28 0-3.89-1.57Q1 16.85 1 14.58q0-1.95 1.17-3.48 1.18-1.53 3.08-1.95.63-2.3 2.5-3.72Q9.63 4 12 4q2.93 0 4.96 2.04Q19 8.07 19 11q1.73.2 2.86 1.5 1.14 1.28 1.14 3 0 1.88-1.31 3.19T18.5 20m-12-2h12q1.05 0 1.77-.73.73-.72.73-1.77t-.73-1.77Q19.55 13 18.5 13H17v-2q0-2.07-1.46-3.54Q14.08 6 12 6 9.93 6 8.46 7.46 7 8.93 7 11h-.5q-1.45 0-2.47 1.03Q3 13.05 3 14.5T4.03 17q1.02 1 2.47 1m5.5-6"/></svg>
<span class="fig-title">Terabytes</span>
<span class="fig-sub">stored in a bucket</span>
<svg class="scale-grid" viewBox="0 0 108 108" style="max-width: 108px"><defs><pattern id="scale-tb" width="12" height="12" patternUnits="userSpaceOnUse"><rect class="native" x="1" y="1" width="10" height="10" rx="1.7"/></pattern></defs><rect fill="url(#scale-tb)" width="108" height="108"/></svg>
<span class="scale-unit">TB</span>
</div>
<div class="scale-col">
<svg class="fig-icon" viewBox="0 0 24 24"><path d="M6.5 20q-2.28 0-3.89-1.57Q1 16.85 1 14.58q0-1.95 1.17-3.48 1.18-1.53 3.08-1.95.63-2.3 2.5-3.72Q9.63 4 12 4q2.93 0 4.96 2.04Q19 8.07 19 11q1.73.2 2.86 1.5 1.14 1.28 1.14 3 0 1.88-1.31 3.19T18.5 20m-12-2h12q1.05 0 1.77-.73.73-.72.73-1.77t-.73-1.77Q19.55 13 18.5 13H17v-2q0-2.07-1.46-3.54Q14.08 6 12 6 9.93 6 8.46 7.46 7 8.93 7 11h-.5q-1.45 0-2.47 1.03Q3 13.05 3 14.5T4.03 17q1.02 1 2.47 1m5.5-6"/></svg>
<span class="fig-title">Petabytes</span>
<span class="fig-sub">referenced in place</span>
<svg class="scale-grid" viewBox="0 0 162 140" style="max-width: 162px"><defs><pattern id="scale-pb" width="14" height="14" patternUnits="userSpaceOnUse"><rect class="virtual netcdf fine" x="1" y="1" width="12" height="12" rx="2"/></pattern></defs><rect fill="url(#scale-pb)" width="140" height="140"/><rect class="native" x="147" y="125" width="14" height="14" rx="2"/></svg>
<span class="fig-sub">Icechunk repository: gigabytes</span>
<span class="scale-unit">PB</span>
</div>
</div>
</div>
</div>

<div class="benefit-row" markdown>
<div class="benefit-text" markdown>

## Zero-Copy Ingest {#zero-copy-ingest}

Reading a TIFF, HDF5, NetCDF or GRIB file from the cloud is slow.
A reader has to fetch headers and offset tables before it finds the bytes it wants.
Each lookup is another round trip, so it is often faster to download the entire file first.

With Icechunk, you parse each file once and record where every chunk's bytes are.
Every reader after that skips the parsing.
Reading a virtual chunk (a pointer to bytes in the original file) is one range request instead of many round trips.
This makes the files cloud-optimized without duplicating or changing the originals.

This technique turned the 115 TB GOES-16 satellite archive (380,000 NetCDF files) into an Icechunk repository of about 80 GB ([blog post](https://www.earthmover.io/blog/virtual-zarr)).

[:octicons-arrow-right-24: Virtual datasets](guides/virtual.md)

</div>
<div class="benefit-figure">
<iframe data-illustrated="virtual/netcdf" title="Virtual chunks in a NetCDF file" loading="lazy"></iframe>
</div>
</div>

<div class="benefit-row benefit-row--sub" markdown>
<div class="benefit-text" markdown>

### Mix Virtual and Native Chunks {#mix-virtual-and-native}

You don't have to choose between virtual and native chunks.
One repository, and even one array, can mix both.
Reference an archive where it sits, then append new data, fix a bad chunk, or add downsampled levels as native chunks.
Anyone reading the repository sees ordinary Zarr and never needs to know which kind of chunk they are reading.

</div>
<div class="benefit-figure">
<div class="fig mix" role="img" aria-label="TIFF files in S3, NetCDF over HTTPS and HDF5 in Google Cloud Storage are referenced in place as virtual chunks, and new data is written as native chunks, all in one array along the time axis.">
<div class="mix-sources">
<div class="ck ck--virtual ck--tiff"><span class="fig-title">TIFF</span><span class="fig-sub">S3 bucket</span></div>
<div class="ck ck--virtual ck--netcdf"><span class="fig-title">NetCDF</span><span class="fig-sub">HTTPS</span></div>
<div class="ck ck--virtual ck--hdf5"><span class="fig-title">HDF5</span><span class="fig-sub">Google Cloud Storage</span></div>
<div class="ck ck--native"><span class="fig-title">New data</span><span class="fig-sub">written by Icechunk</span></div>
</div>
<div class="mix-array">
<span class="fig-title">one Icechunk array</span>
<div class="mix-cells">
<span class="ck ck--virtual ck--tiff"></span><span class="ck ck--virtual ck--tiff"></span><span class="ck ck--virtual ck--tiff"></span><span class="ck ck--virtual ck--netcdf"></span><span class="ck ck--virtual ck--netcdf"></span><span class="ck ck--virtual ck--netcdf"></span><span class="ck ck--virtual ck--hdf5"></span><span class="ck ck--virtual ck--hdf5"></span><span class="ck ck--virtual ck--hdf5"></span><span class="ck ck--native"></span><span class="ck ck--native"></span><span class="ck ck--native"></span>
</div>
<div class="mix-ticks">
<span class="fig-mono">t0</span><span class="fig-mono">t1</span><span class="fig-mono">t2</span><span class="fig-mono">t3</span><span class="fig-mono">t4</span><span class="fig-mono">t5</span><span class="fig-mono">t6</span><span class="fig-mono">t7</span><span class="fig-mono">t8</span><span class="fig-mono">t9</span><span class="fig-mono">t10</span><span class="fig-mono">t11</span>
</div>
<div class="fig-legend">
<span><span class="ck ck--virtual ck--legend"></span>dashed: virtual, read from the original file</span>
<span><span class="ck ck--native"></span>solid: native, stored by Icechunk</span>
</div>
</div>
</div>
</div>
</div>

<div class="benefit-row" markdown>
<div class="benefit-text" markdown>

## Versioning {#versioning}

In a plain Zarr store, a bad write or an accidental delete is permanent.
Icechunk records every change as a commit, so you can undo a mistake by moving the branch back to an earlier commit.
Old commits stay readable until you choose to expire them.

Tag a commit to cite the exact data in a paper, or create a branch to try a change without affecting anyone else.

[:octicons-arrow-right-24: Version control](understanding/version-control.md)

</div>
<div class="benefit-figure">
<svg class="fig fig-wide" viewBox="0 0 760 262" role="img" aria-label="Four commits on main, a tag v1 on the second, and a branch named experiment off the third"><line class="line" x1="145.0" y1="110" x2="525.0" y2="110"/><path class="line" d="M420 130.0 C 420 195, 480 195, 525.0 195"/><rect class="box" x="15.0" y="90.0" width="130" height="40" rx="8"/><text class="label" x="80" y="115">create</text><rect class="box" x="185.0" y="90.0" width="130" height="40" rx="8"/><text class="label" x="250" y="115">add January</text><rect class="box" x="355.0" y="90.0" width="130" height="40" rx="8"/><text class="label" x="420" y="115">add February</text><rect class="box" x="525.0" y="90.0" width="130" height="40" rx="8"/><text class="label" x="590" y="115">fix February</text><rect class="box" x="525.0" y="175.0" width="130" height="40" rx="8"/><text class="label" x="590" y="200">try a new mask</text><line class="dashed" x1="590" y1="52" x2="590" y2="90.0"/><rect class="branch" x="561.0" y="28" width="58.0" height="24" rx="12"/><text class="on-color pill" x="590" y="45">main</text><line class="dashed" x1="250" y1="52" x2="250" y2="90.0"/><rect class="tag" x="208.25" y="28" width="83.5" height="24" rx="12"/><text class="on-color pill" x="250" y="45">tag: v1</text><line class="dashed" x1="590" y1="215" x2="590" y2="226"/><rect class="branch" x="538" y="226" width="104" height="24" rx="12"/><text class="on-color pill" x="590" y="243">experiment</text></svg>
<svg class="fig fig-narrow" viewBox="0 0 340 320" role="img" aria-label="Four commits on main, a tag v1 on the second, and a branch named experiment off the third"><line class="line" x1="90" y1="60" x2="90" y2="230"/><path class="line" d="M160 180 C 230 180, 254 190, 254 230"/><rect class="box" x="20" y="20" width="140" height="40" rx="8"/><text class="label" x="90" y="45">create</text><rect class="box" x="20" y="90" width="140" height="40" rx="8"/><text class="label" x="90" y="115">add January</text><rect class="box" x="20" y="160" width="140" height="40" rx="8"/><text class="label" x="90" y="185">add February</text><rect class="box" x="20" y="230" width="140" height="40" rx="8"/><text class="label" x="90" y="255">fix February</text><rect class="box" x="184" y="230" width="140" height="40" rx="8"/><text class="label" x="254" y="255">try a new mask</text><line class="dashed" x1="160" y1="110" x2="184" y2="110"/><rect class="tag" x="184" y="98" width="83.5" height="24" rx="12"/><text class="on-color pill" x="225.75" y="115">tag: v1</text><line class="dashed" x1="90" y1="270" x2="90" y2="284"/><rect class="branch" x="61" y="284" width="58" height="24" rx="12"/><text class="on-color pill" x="90" y="301">main</text><line class="dashed" x1="254" y1="270" x2="254" y2="284"/><rect class="branch" x="202" y="284" width="104" height="24" rx="12"/><text class="on-color pill" x="254" y="301">experiment</text></svg>
</div>
</div>

<div class="benefit-row benefit-row--sub" markdown>
<div class="benefit-text" markdown>

### History Without Ballooning Storage {#history-storage}

Keeping history doesn't mean keeping full copies.
A commit writes only the chunks that changed, and every other chunk points at data already in storage.

[:octicons-arrow-right-24: Expiring old data](understanding/expiration.md)

</div>
<div class="benefit-figure">
<iframe data-illustrated="commits" title="Commits store only the chunks that changed" loading="lazy"></iframe>
</div>
</div>

<div class="benefit-row" markdown>
<div class="benefit-text" markdown>

## ACID Transactions {#consistency}

Without transactions, a reader can catch a write halfway.
The result is a Schrödinger's array, but unlike Schrödinger's cat, looking at it doesn't settle it into one state.
You can get some chunks from before the write and some from after, an array that never existed.
The animation shows a reader catching one.

With Icechunk, a reader always sees one complete commit, even while another process is writing.
When two writers commit at once, the second is rejected or rebased, never silently overwritten.

[:octicons-arrow-right-24: Transactions](understanding/concepts.md#transactions)

</div>
<div class="benefit-figure">
<iframe data-illustrated="consistency" data-params="chunks=6&write=15" title="Torn reads versus consistent reads" loading="lazy"></iframe>
</div>
</div>

<div class="benefit-row" markdown>
<div class="benefit-text" markdown>

## No Server Required {#format-and-client-only}

The Icechunk library runs inside your code and reads and writes files directly.
There is no server, database or service to run.

- [The format spec](reference/spec-v2-1.md)
- Libraries: [Python](reference/index.md) · [Rust](reference/icechunk-rust.md) · [JavaScript](reference/icechunk-js.md) · [more languages](languages.md)

</div>
<div class="benefit-figure">
<div class="fig ns" role="img" aria-label="Your computer runs your code and the Icechunk library, available in Python, Rust, JavaScript, R, Java and Julia. The library reads and writes files on a local disk or in cloud object storage. There is no server in between.">
<div class="ns-computer">
<div class="ns-head"><svg class="fig-icon" viewBox="0 0 24 24"><path d="M4 6h16v10H4m16 2a2 2 0 0 0 2-2V6a2 2 0 0 0-2-2H4c-1.11 0-2 .89-2 2v10a2 2 0 0 0 2 2H0v2h24v-2z"/></svg><span class="fig-title">Your computer</span></div>
<div class="ns-code"><span class="fig-label">your code</span><span class="fig-sub">notebooks, pipelines, apps, viewers</span></div>
<div class="ns-lib">Icechunk library</div>
<div class="ns-langs"><span class="fig-sub">in</span><span class="fig-chip">Python</span><span class="fig-chip">Rust</span><span class="fig-chip">JavaScript</span><span class="fig-chip">R</span><span class="fig-chip">Java</span><span class="fig-chip">Julia</span></div>
</div>
<div class="ns-io">
<svg class="ns-arrow" viewBox="0 0 100 20"><line x1="6" y1="10" x2="94" y2="10"/><path d="M0 10 10 3v14zM100 10 90 3v14z"/></svg>
<span class="fig-sub">reads and writes files</span>
</div>
<div class="ns-stores">
<div class="ns-store"><svg class="fig-icon" viewBox="0 0 24 24"><path d="M6.5 20q-2.28 0-3.89-1.57Q1 16.85 1 14.58q0-1.95 1.17-3.48 1.18-1.53 3.08-1.95.63-2.3 2.5-3.72Q9.63 4 12 4q2.93 0 4.96 2.04Q19 8.07 19 11q1.73.2 2.86 1.5 1.14 1.28 1.14 3 0 1.88-1.31 3.19T18.5 20m-12-2h12q1.05 0 1.77-.73.73-.72.73-1.77t-.73-1.77Q19.55 13 18.5 13H17v-2q0-2.07-1.46-3.54Q14.08 6 12 6 9.93 6 8.46 7.46 7 8.93 7 11h-.5q-1.45 0-2.47 1.03Q3 13.05 3 14.5T4.03 17q1.02 1 2.47 1m5.5-6"/></svg><span class="fig-title">Cloud</span><span class="fig-sub">S3, GCS, Azure, …</span></div>
<span class="ns-or">or</span>
<div class="ns-store"><svg class="fig-icon" viewBox="0 0 24 24"><path d="M6 2h12a2 2 0 0 1 2 2v16a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2m6 2a6 6 0 0 0-6 6c0 3.31 2.69 6 6.1 6l-.88-2.23a1.01 1.01 0 0 1 .37-1.37l.86-.5a1.01 1.01 0 0 1 1.37.37l1.92 2.42A5.98 5.98 0 0 0 18 10a6 6 0 0 0-6-6m0 5a1 1 0 0 1 1 1 1 1 0 0 1-1 1 1 1 0 0 1-1-1 1 1 0 0 1 1-1m-5 9a1 1 0 0 0-1 1 1 1 0 0 0 1 1 1 1 0 0 0 1-1 1 1 0 0 0-1-1m5.09-4.73 2.49 6.31 2.59-1.5-4.22-5.31z"/></svg><span class="fig-title">Local disk</span><span class="fig-sub">your laptop or a cluster</span></div>
</div>
</div>
</div>
</div>
