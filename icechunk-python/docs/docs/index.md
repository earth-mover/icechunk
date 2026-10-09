---
template: home.html
title: Icechunk - Open-source, high-performance array format and library for scientific data
---

<div class="benefit-row" markdown>
<div class="benefit-text" markdown>

## Scale {#scale}

Store gigabytes on your laptop or terabytes in a bucket, or reference petabytes of existing files with a repository of a few gigabytes. The same format and library cover all three.

- A read fetches only the chunks it touches, and opening a repository never lists the bucket.
- Each commit stores only the chunks that changed, so keeping history doesn't multiply storage.
- **NASA**'s virtual stores feasibility report recommends adopting Icechunk ([report](https://nasa-impact.github.io/virtual-stores-feasibility-report/recommendations.html)).
- **NOAA** forecast and radar archives (HRRR, GFS, MRMS) are published as Icechunk on the AWS Open Data Registry ([dynamical.org](https://registry.opendata.aws/dynamical-noaa-mrms/)).

[:octicons-arrow-right-24: Tuning performance](guides/performance.md) ·
[:octicons-arrow-right-24: Parallel writes](understanding/parallel.md)

</div>
<div class="benefit-figure">
<svg class="fig scalefig" viewBox="0 0 760 330" role="img" aria-label="Icechunk stores gigabytes on a laptop disk or terabytes in a bucket as native chunks, and references petabytes of existing files in place with a repository of only a few gigabytes."><line class="line" x1="30" y1="292" x2="730" y2="292"/><path class="icon" transform="translate(87 8) scale(1.5)" d="M6 2h12a2 2 0 0 1 2 2v16a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2m6 2a6 6 0 0 0-6 6c0 3.31 2.69 6 6.1 6l-.88-2.23a1.01 1.01 0 0 1 .37-1.37l.86-.5a1.01 1.01 0 0 1 1.37.37l1.92 2.42A5.98 5.98 0 0 0 18 10a6 6 0 0 0-6-6m0 5a1 1 0 0 1 1 1 1 1 0 0 1-1 1 1 1 0 0 1-1-1 1 1 0 0 1 1-1m-5 9a1 1 0 0 0-1 1 1 1 0 0 0 1 1 1 1 0 0 0 1-1 1 1 0 0 0-1-1m5.09-4.73 2.49 6.31 2.59-1.5-4.22-5.31z"/><text class="title" x="105" y="62">Gigabytes</text><text class="sub" x="105" y="80">stored on your laptop's disk</text><rect class="native" x="63.0" y="95.0" width="18" height="18" rx="3.0"/><rect class="native" x="85.0" y="95.0" width="18" height="18" rx="3.0"/><rect class="native" x="107.0" y="95.0" width="18" height="18" rx="3.0"/><rect class="native" x="129.0" y="95.0" width="18" height="18" rx="3.0"/><rect class="native" x="63.0" y="117.0" width="18" height="18" rx="3.0"/><rect class="native" x="85.0" y="117.0" width="18" height="18" rx="3.0"/><rect class="native" x="107.0" y="117.0" width="18" height="18" rx="3.0"/><rect class="native" x="129.0" y="117.0" width="18" height="18" rx="3.0"/><rect class="native" x="63.0" y="139.0" width="18" height="18" rx="3.0"/><rect class="native" x="85.0" y="139.0" width="18" height="18" rx="3.0"/><rect class="native" x="107.0" y="139.0" width="18" height="18" rx="3.0"/><rect class="native" x="129.0" y="139.0" width="18" height="18" rx="3.0"/><rect class="native" x="63.0" y="161.0" width="18" height="18" rx="3.0"/><rect class="native" x="85.0" y="161.0" width="18" height="18" rx="3.0"/><rect class="native" x="107.0" y="161.0" width="18" height="18" rx="3.0"/><rect class="native" x="129.0" y="161.0" width="18" height="18" rx="3.0"/><line class="frame" x1="105" y1="288" x2="105" y2="296"/><text class="title" x="105" y="318">GB</text><path class="icon" transform="translate(312 8) scale(1.5)" d="M6.5 20q-2.28 0-3.89-1.57Q1 16.85 1 14.58q0-1.95 1.17-3.48 1.18-1.53 3.08-1.95.63-2.3 2.5-3.72Q9.63 4 12 4q2.93 0 4.96 2.04Q19 8.07 19 11q1.73.2 2.86 1.5 1.14 1.28 1.14 3 0 1.88-1.31 3.19T18.5 20m-12-2h12q1.05 0 1.77-.73.73-.72.73-1.77t-.73-1.77Q19.55 13 18.5 13H17v-2q0-2.07-1.46-3.54Q14.08 6 12 6 9.93 6 8.46 7.46 7 8.93 7 11h-.5q-1.45 0-2.47 1.03Q3 13.05 3 14.5T4.03 17q1.02 1 2.47 1m5.5-6"/><text class="title" x="330" y="62">Terabytes</text><text class="sub" x="330" y="80">stored in a bucket</text><rect class="native" x="277.0" y="95.0" width="10" height="10" rx="1.7"/><rect class="native" x="289.0" y="95.0" width="10" height="10" rx="1.7"/><rect class="native" x="301.0" y="95.0" width="10" height="10" rx="1.7"/><rect class="native" x="313.0" y="95.0" width="10" height="10" rx="1.7"/><rect class="native" x="325.0" y="95.0" width="10" height="10" rx="1.7"/><rect class="native" x="337.0" y="95.0" width="10" height="10" rx="1.7"/><rect class="native" x="349.0" y="95.0" width="10" height="10" rx="1.7"/><rect class="native" x="361.0" y="95.0" width="10" height="10" rx="1.7"/><rect class="native" x="373.0" y="95.0" width="10" height="10" rx="1.7"/><rect class="native" x="277.0" y="107.0" width="10" height="10" rx="1.7"/><rect class="native" x="289.0" y="107.0" width="10" height="10" rx="1.7"/><rect class="native" x="301.0" y="107.0" width="10" height="10" rx="1.7"/><rect class="native" x="313.0" y="107.0" width="10" height="10" rx="1.7"/><rect class="native" x="325.0" y="107.0" width="10" height="10" rx="1.7"/><rect class="native" x="337.0" y="107.0" width="10" height="10" rx="1.7"/><rect class="native" x="349.0" y="107.0" width="10" height="10" rx="1.7"/><rect class="native" x="361.0" y="107.0" width="10" height="10" rx="1.7"/><rect class="native" x="373.0" y="107.0" width="10" height="10" rx="1.7"/><rect class="native" x="277.0" y="119.0" width="10" height="10" rx="1.7"/><rect class="native" x="289.0" y="119.0" width="10" height="10" rx="1.7"/><rect class="native" x="301.0" y="119.0" width="10" height="10" rx="1.7"/><rect class="native" x="313.0" y="119.0" width="10" height="10" rx="1.7"/><rect class="native" x="325.0" y="119.0" width="10" height="10" rx="1.7"/><rect class="native" x="337.0" y="119.0" width="10" height="10" rx="1.7"/><rect class="native" x="349.0" y="119.0" width="10" height="10" rx="1.7"/><rect class="native" x="361.0" y="119.0" width="10" height="10" rx="1.7"/><rect class="native" x="373.0" y="119.0" width="10" height="10" rx="1.7"/><rect class="native" x="277.0" y="131.0" width="10" height="10" rx="1.7"/><rect class="native" x="289.0" y="131.0" width="10" height="10" rx="1.7"/><rect class="native" x="301.0" y="131.0" width="10" height="10" rx="1.7"/><rect class="native" x="313.0" y="131.0" width="10" height="10" rx="1.7"/><rect class="native" x="325.0" y="131.0" width="10" height="10" rx="1.7"/><rect class="native" x="337.0" y="131.0" width="10" height="10" rx="1.7"/><rect class="native" x="349.0" y="131.0" width="10" height="10" rx="1.7"/><rect class="native" x="361.0" y="131.0" width="10" height="10" rx="1.7"/><rect class="native" x="373.0" y="131.0" width="10" height="10" rx="1.7"/><rect class="native" x="277.0" y="143.0" width="10" height="10" rx="1.7"/><rect class="native" x="289.0" y="143.0" width="10" height="10" rx="1.7"/><rect class="native" x="301.0" y="143.0" width="10" height="10" rx="1.7"/><rect class="native" x="313.0" y="143.0" width="10" height="10" rx="1.7"/><rect class="native" x="325.0" y="143.0" width="10" height="10" rx="1.7"/><rect class="native" x="337.0" y="143.0" width="10" height="10" rx="1.7"/><rect class="native" x="349.0" y="143.0" width="10" height="10" rx="1.7"/><rect class="native" x="361.0" y="143.0" width="10" height="10" rx="1.7"/><rect class="native" x="373.0" y="143.0" width="10" height="10" rx="1.7"/><rect class="native" x="277.0" y="155.0" width="10" height="10" rx="1.7"/><rect class="native" x="289.0" y="155.0" width="10" height="10" rx="1.7"/><rect class="native" x="301.0" y="155.0" width="10" height="10" rx="1.7"/><rect class="native" x="313.0" y="155.0" width="10" height="10" rx="1.7"/><rect class="native" x="325.0" y="155.0" width="10" height="10" rx="1.7"/><rect class="native" x="337.0" y="155.0" width="10" height="10" rx="1.7"/><rect class="native" x="349.0" y="155.0" width="10" height="10" rx="1.7"/><rect class="native" x="361.0" y="155.0" width="10" height="10" rx="1.7"/><rect class="native" x="373.0" y="155.0" width="10" height="10" rx="1.7"/><rect class="native" x="277.0" y="167.0" width="10" height="10" rx="1.7"/><rect class="native" x="289.0" y="167.0" width="10" height="10" rx="1.7"/><rect class="native" x="301.0" y="167.0" width="10" height="10" rx="1.7"/><rect class="native" x="313.0" y="167.0" width="10" height="10" rx="1.7"/><rect class="native" x="325.0" y="167.0" width="10" height="10" rx="1.7"/><rect class="native" x="337.0" y="167.0" width="10" height="10" rx="1.7"/><rect class="native" x="349.0" y="167.0" width="10" height="10" rx="1.7"/><rect class="native" x="361.0" y="167.0" width="10" height="10" rx="1.7"/><rect class="native" x="373.0" y="167.0" width="10" height="10" rx="1.7"/><rect class="native" x="277.0" y="179.0" width="10" height="10" rx="1.7"/><rect class="native" x="289.0" y="179.0" width="10" height="10" rx="1.7"/><rect class="native" x="301.0" y="179.0" width="10" height="10" rx="1.7"/><rect class="native" x="313.0" y="179.0" width="10" height="10" rx="1.7"/><rect class="native" x="325.0" y="179.0" width="10" height="10" rx="1.7"/><rect class="native" x="337.0" y="179.0" width="10" height="10" rx="1.7"/><rect class="native" x="349.0" y="179.0" width="10" height="10" rx="1.7"/><rect class="native" x="361.0" y="179.0" width="10" height="10" rx="1.7"/><rect class="native" x="373.0" y="179.0" width="10" height="10" rx="1.7"/><rect class="native" x="277.0" y="191.0" width="10" height="10" rx="1.7"/><rect class="native" x="289.0" y="191.0" width="10" height="10" rx="1.7"/><rect class="native" x="301.0" y="191.0" width="10" height="10" rx="1.7"/><rect class="native" x="313.0" y="191.0" width="10" height="10" rx="1.7"/><rect class="native" x="325.0" y="191.0" width="10" height="10" rx="1.7"/><rect class="native" x="337.0" y="191.0" width="10" height="10" rx="1.7"/><rect class="native" x="349.0" y="191.0" width="10" height="10" rx="1.7"/><rect class="native" x="361.0" y="191.0" width="10" height="10" rx="1.7"/><rect class="native" x="373.0" y="191.0" width="10" height="10" rx="1.7"/><line class="frame" x1="330" y1="288" x2="330" y2="296"/><text class="title" x="330" y="318">TB</text><path class="icon" transform="translate(572 8) scale(1.5)" d="M6.5 20q-2.28 0-3.89-1.57Q1 16.85 1 14.58q0-1.95 1.17-3.48 1.18-1.53 3.08-1.95.63-2.3 2.5-3.72Q9.63 4 12 4q2.93 0 4.96 2.04Q19 8.07 19 11q1.73.2 2.86 1.5 1.14 1.28 1.14 3 0 1.88-1.31 3.19T18.5 20m-12-2h12q1.05 0 1.77-.73.73-.72.73-1.77t-.73-1.77Q19.55 13 18.5 13H17v-2q0-2.07-1.46-3.54Q14.08 6 12 6 9.93 6 8.46 7.46 7 8.93 7 11h-.5q-1.45 0-2.47 1.03Q3 13.05 3 14.5T4.03 17q1.02 1 2.47 1m5.5-6"/><text class="title" x="590" y="62">Petabytes</text><text class="sub" x="590" y="80">referenced in place</text><rect class="virtual netcdf fine" x="521.0" y="95.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="535.0" y="95.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="549.0" y="95.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="563.0" y="95.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="577.0" y="95.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="591.0" y="95.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="605.0" y="95.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="619.0" y="95.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="633.0" y="95.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="647.0" y="95.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="521.0" y="109.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="535.0" y="109.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="549.0" y="109.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="563.0" y="109.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="577.0" y="109.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="591.0" y="109.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="605.0" y="109.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="619.0" y="109.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="633.0" y="109.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="647.0" y="109.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="521.0" y="123.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="535.0" y="123.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="549.0" y="123.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="563.0" y="123.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="577.0" y="123.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="591.0" y="123.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="605.0" y="123.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="619.0" y="123.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="633.0" y="123.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="647.0" y="123.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="521.0" y="137.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="535.0" y="137.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="549.0" y="137.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="563.0" y="137.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="577.0" y="137.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="591.0" y="137.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="605.0" y="137.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="619.0" y="137.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="633.0" y="137.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="647.0" y="137.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="521.0" y="151.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="535.0" y="151.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="549.0" y="151.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="563.0" y="151.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="577.0" y="151.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="591.0" y="151.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="605.0" y="151.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="619.0" y="151.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="633.0" y="151.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="647.0" y="151.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="521.0" y="165.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="535.0" y="165.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="549.0" y="165.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="563.0" y="165.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="577.0" y="165.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="591.0" y="165.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="605.0" y="165.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="619.0" y="165.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="633.0" y="165.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="647.0" y="165.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="521.0" y="179.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="535.0" y="179.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="549.0" y="179.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="563.0" y="179.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="577.0" y="179.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="591.0" y="179.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="605.0" y="179.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="619.0" y="179.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="633.0" y="179.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="647.0" y="179.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="521.0" y="193.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="535.0" y="193.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="549.0" y="193.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="563.0" y="193.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="577.0" y="193.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="591.0" y="193.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="605.0" y="193.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="619.0" y="193.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="633.0" y="193.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="647.0" y="193.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="521.0" y="207.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="535.0" y="207.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="549.0" y="207.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="563.0" y="207.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="577.0" y="207.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="591.0" y="207.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="605.0" y="207.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="619.0" y="207.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="633.0" y="207.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="647.0" y="207.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="521.0" y="221.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="535.0" y="221.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="549.0" y="221.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="563.0" y="221.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="577.0" y="221.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="591.0" y="221.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="605.0" y="221.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="619.0" y="221.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="633.0" y="221.0" width="12" height="12" rx="2"/><rect class="virtual netcdf fine" x="647.0" y="221.0" width="12" height="12" rx="2"/><rect class="native" x="671.0" y="219" width="14" height="14" rx="2"/><text class="sub" x="590" y="255">Icechunk repository: a few GB</text><line class="frame" x1="590" y1="288" x2="590" y2="296"/><text class="title" x="590" y="318">PB</text></svg>
</div>
</div>

<div class="benefit-row" markdown>
<div class="benefit-text" markdown>

## Zero-copy ingest {#zero-copy-ingest}

Reading a TIFF, HDF5, NetCDF or GRIB file from the cloud is slow.
A reader has to fetch headers and offset tables before it finds the bytes it wants.

With Icechunk, that cost is paid once, by whoever writes the repository.
Every reader after that gets each chunk in one range request to the original file.
The files are not copied or changed.

The GOES-16 satellite archive, 115 TB in 380,000 NetCDF files, became an 80 GB Icechunk repository this way ([blog post](https://www.earthmover.io/blog/virtual-zarr)).

[:octicons-arrow-right-24: Virtual datasets](guides/virtual.md)

</div>
<div class="benefit-figure">
<iframe data-illustrated="virtual/netcdf" title="Virtual chunks in a NetCDF file" loading="lazy"></iframe>
</div>
</div>

<div class="benefit-row" markdown>
<div class="benefit-text" markdown>

### Mix virtual and native chunks {#mix-virtual-and-native}

One array can hold both.
Reference the archive where it sits, then append new data, fix a bad chunk, or add a downsampled level as native chunks.
Readers see one array and never need to know which is which.

</div>
<div class="benefit-figure">
<svg class="fig" viewBox="0 0 820 300" role="img" aria-label="TIFF files in S3, NetCDF over HTTPS and HDF5 in Google Cloud Storage are referenced in place as virtual chunks, and new data is written as native chunks, all in one array along the time axis."><rect class="virtual tiff" x="20" y="40" width="250" height="52" rx="8"/><text class="title start" x="36" y="62">TIFF</text><text class="sub start" x="36" y="81">S3 bucket</text><rect class="virtual netcdf" x="20" y="106" width="250" height="52" rx="8"/><text class="title start" x="36" y="128">NetCDF</text><text class="sub start" x="36" y="147">HTTPS</text><rect class="virtual hdf5" x="20" y="172" width="250" height="52" rx="8"/><text class="title start" x="36" y="194">HDF5</text><text class="sub start" x="36" y="213">Google Cloud Storage</text><rect class="native" x="20" y="238" width="250" height="52" rx="8"/><text class="title start on-native" x="36" y="260">New data</text><text class="sub start on-native" x="36" y="279">written by Icechunk</text><rect class="virtual tiff" x="330" y="118" width="34" height="52" rx="4"/><text class="mono" x="347.0" y="188">t0</text><rect class="virtual tiff" x="370" y="118" width="34" height="52" rx="4"/><text class="mono" x="387.0" y="188">t1</text><rect class="virtual tiff" x="410" y="118" width="34" height="52" rx="4"/><text class="mono" x="427.0" y="188">t2</text><rect class="virtual netcdf" x="450" y="118" width="34" height="52" rx="4"/><text class="mono" x="467.0" y="188">t3</text><rect class="virtual netcdf" x="490" y="118" width="34" height="52" rx="4"/><text class="mono" x="507.0" y="188">t4</text><rect class="virtual netcdf" x="530" y="118" width="34" height="52" rx="4"/><text class="mono" x="547.0" y="188">t5</text><rect class="virtual hdf5" x="570" y="118" width="34" height="52" rx="4"/><text class="mono" x="587.0" y="188">t6</text><rect class="virtual hdf5" x="610" y="118" width="34" height="52" rx="4"/><text class="mono" x="627.0" y="188">t7</text><rect class="virtual hdf5" x="650" y="118" width="34" height="52" rx="4"/><text class="mono" x="667.0" y="188">t8</text><rect class="native" x="690" y="118" width="34" height="52" rx="4"/><text class="mono" x="707.0" y="188">t9</text><rect class="native" x="730" y="118" width="34" height="52" rx="4"/><text class="mono" x="747.0" y="188">t10</text><rect class="native" x="770" y="118" width="34" height="52" rx="4"/><text class="mono" x="787.0" y="188">t11</text><rect class="frame" x="322" y="110" width="490" height="68" rx="8"/><text class="title" x="567.0" y="96">one Icechunk array</text><rect class="virtual legend" x="330" y="215" width="18" height="18" rx="3"/><text class="sub start" x="356" y="229">dashed: virtual, read from the original file</text><rect class="native" x="330" y="243" width="18" height="18" rx="3"/><text class="sub start" x="356" y="257">solid: native, stored by Icechunk</text></svg>
</div>
</div>

<div class="benefit-row" markdown>
<div class="benefit-text" markdown>

## Versioning {#versioning}

Every write to an Icechunk repository is a commit, deletes included.
A bad write or an accidental delete is undone by moving the branch back to an earlier commit.
Old commits stay readable until you choose to expire them.

Tag a commit to cite it in a paper, or branch to try a change without affecting anyone else.

[:octicons-arrow-right-24: Version control](understanding/version-control.md)

</div>
<div class="benefit-figure">
<svg class="fig" viewBox="0 0 760 262" role="img" aria-label="Four commits on main, a tag v1 on the second, and a branch named experiment off the third"><line class="line" x1="145.0" y1="110" x2="525.0" y2="110"/><path class="line" d="M420 130.0 C 420 195, 480 195, 525.0 195"/><rect class="box" x="15.0" y="90.0" width="130" height="40" rx="8"/><text class="label" x="80" y="115">create</text><rect class="box" x="185.0" y="90.0" width="130" height="40" rx="8"/><text class="label" x="250" y="115">add January</text><rect class="box" x="355.0" y="90.0" width="130" height="40" rx="8"/><text class="label" x="420" y="115">add February</text><rect class="box" x="525.0" y="90.0" width="130" height="40" rx="8"/><text class="label" x="590" y="115">fix February</text><rect class="box" x="525.0" y="175.0" width="130" height="40" rx="8"/><text class="label" x="590" y="200">try a new mask</text><line class="dashed" x1="590" y1="52" x2="590" y2="90.0"/><rect class="branch" x="561.0" y="28" width="58.0" height="24" rx="12"/><text class="on-color pill" x="590" y="45">main</text><line class="dashed" x1="250" y1="52" x2="250" y2="90.0"/><rect class="tag" x="208.25" y="28" width="83.5" height="24" rx="12"/><text class="on-color pill" x="250" y="45">tag: v1</text><line class="dashed" x1="590" y1="215" x2="590" y2="226"/><rect class="branch" x="538" y="226" width="104" height="24" rx="12"/><text class="on-color pill" x="590" y="243">experiment</text></svg>
</div>
</div>

<div class="benefit-row" markdown>
<div class="benefit-text" markdown>

### History without ballooning storage {#history-storage}

A commit writes only the chunks that changed.
Every other chunk in the new snapshot points at data already in storage, so a long history costs a fraction of keeping full copies.

[:octicons-arrow-right-24: Expiring old data](understanding/expiration.md)

</div>
<div class="benefit-figure">
<iframe data-illustrated="commits" title="Commits store only the chunks that changed" loading="lazy"></iframe>
</div>
</div>

<div class="benefit-row" markdown>
<div class="benefit-text" markdown>

## Consistency {#consistency}

Icechunk gives you ACID transactions on arrays.
Without them, a reader can catch a write halfway and get arrays that don't match.

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

## Format and client only {#format-and-client-only}

The Icechunk library runs inside your program and reads and writes files directly.
There is no server, database or service to run.

- [The format spec](reference/spec-v2-1.md)
- Libraries: [Python](reference/index.md) · [Rust](reference/icechunk-rust.md) · [JavaScript](reference/icechunk-js.md) · [more languages](languages.md)

</div>
<div class="benefit-figure">
<svg class="fig" viewBox="0 0 760 320" role="img" aria-label="Your computer runs your code and the Icechunk library, available in Python, Rust, JavaScript, R, Java and Julia. The library reads and writes files on a local disk or in cloud object storage. There is no server in between.">
<defs><marker id="fig-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" class="head"/></marker></defs>
<rect class="frame" x="20" y="20" width="400" height="285" rx="14"/>
<path class="icon" transform="translate(42 34) scale(1.6)" d="M4 6h16v10H4m16 2a2 2 0 0 0 2-2V6a2 2 0 0 0-2-2H4c-1.11 0-2 .89-2 2v10a2 2 0 0 0 2 2H0v2h24v-2z"/>
<text class="title start" x="90" y="60">Your computer</text>
<rect class="box" x="48" y="88" width="344" height="56" rx="8"/>
<text class="label" x="220" y="113">your code</text>
<text class="sub" x="220" y="132">notebooks, pipelines, apps, viewers</text>
<line class="frame" x1="220" y1="144" x2="220" y2="166"/>
<rect class="accent" x="48" y="166" width="344" height="56" rx="8"/>
<text class="on-color title" x="220" y="200">Icechunk library</text>
<text class="sub start" x="48" y="252">in</text>
<rect class="chip" x="48" y="262" width="64" height="24" rx="12"/><text class="chip-text" x="80" y="279">Python</text><rect class="chip" x="118" y="262" width="48" height="24" rx="12"/><text class="chip-text" x="142" y="279">Rust</text><rect class="chip" x="172" y="262" width="94" height="24" rx="12"/><text class="chip-text" x="219" y="279">JavaScript</text><rect class="chip" x="272" y="262" width="26" height="24" rx="12"/><text class="chip-text" x="285" y="279">R</text><rect class="chip" x="304" y="262" width="48" height="24" rx="12"/><text class="chip-text" x="328" y="279">Java</text><rect class="chip" x="358" y="262" width="56" height="24" rx="12"/><text class="chip-text" x="386" y="279">Julia</text>
<line class="arrow" x1="404" y1="182" x2="522" y2="118" marker-end="url(#fig-arrow)" marker-start="url(#fig-arrow)"/>
<line class="arrow" x1="404" y1="206" x2="522" y2="240" marker-end="url(#fig-arrow)" marker-start="url(#fig-arrow)"/>
<text class="sub" x="478" y="190">reads and</text>
<text class="sub" x="478" y="207">writes files</text>
<path class="icon" transform="translate(532 92) scale(2.2)" d="M6.5 20q-2.28 0-3.89-1.57Q1 16.85 1 14.58q0-1.95 1.17-3.48 1.18-1.53 3.08-1.95.63-2.3 2.5-3.72Q9.63 4 12 4q2.93 0 4.96 2.04Q19 8.07 19 11q1.73.2 2.86 1.5 1.14 1.28 1.14 3 0 1.88-1.31 3.19T18.5 20m-12-2h12q1.05 0 1.77-.73.73-.72.73-1.77t-.73-1.77Q19.55 13 18.5 13H17v-2q0-2.07-1.46-3.54Q14.08 6 12 6 9.93 6 8.46 7.46 7 8.93 7 11h-.5q-1.45 0-2.47 1.03Q3 13.05 3 14.5T4.03 17q1.02 1 2.47 1m5.5-6"/>
<text class="title start" x="600" y="114">Cloud</text>
<text class="sub start" x="600" y="134">S3, GCS, Azure, …</text>
<text class="or" x="585" y="186">or</text>
<path class="icon" transform="translate(535 214) scale(2)" d="M6 2h12a2 2 0 0 1 2 2v16a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2m6 2a6 6 0 0 0-6 6c0 3.31 2.69 6 6.1 6l-.88-2.23a1.01 1.01 0 0 1 .37-1.37l.86-.5a1.01 1.01 0 0 1 1.37.37l1.92 2.42A5.98 5.98 0 0 0 18 10a6 6 0 0 0-6-6m0 5a1 1 0 0 1 1 1 1 1 0 0 1-1 1 1 1 0 0 1-1-1 1 1 0 0 1 1-1m-5 9a1 1 0 0 0-1 1 1 1 0 0 0 1 1 1 1 0 0 0 1-1 1 1 0 0 0-1-1m5.09-4.73 2.49 6.31 2.59-1.5-4.22-5.31z"/>
<text class="title start" x="600" y="238">Local disk</text>
<text class="sub start" x="600" y="258">your laptop or a cluster</text>
</svg>
</div>
</div>
