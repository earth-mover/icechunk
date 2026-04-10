---
title: Arraylake
---

# Arraylake

[Icechunk](https://icechunk.io) is a free, open-source (Apache 2.0) transactional storage engine for Zarr.
[Arraylake](https://docs.earthmover.io) is Earthmover's managed cloud platform, built on top of Icechunk.
Both are made by [Earthmover](https://earthmover.io/).

Icechunk is a fully capable, standalone product -- you never need Arraylake to use it.
Arraylake adds operational, collaboration, and data delivery features on top of Icechunk for teams that want a managed experience.

Migrating from Icechunk to Arraylake is easy, as any Icechunk Repository can be directly imported into Arraylake.

## Feature Comparison

### Open-Source Storage Engine

These core capabilities ship with Icechunk and are available in both Icechunk and Arraylake.

| Feature | Icechunk | Arraylake |
|---|---|---|
| Transactional storage engine | :material-check: | :material-check: Built on Icechunk |
| Version control (branches, tags, time travel) | :material-check: | :material-check: |
| ACID transactions with serializable isolation | :material-check: | :material-check: |
| Virtual chunk references (HDF5, NetCDF, GRIB, TIFF) | :material-check: | :material-check: |
| Parallel / distributed writes | :material-check: | :material-check: |
| Cloud storage (S3, GCS, Azure, R2, etc.) | :material-check: Self-managed | :material-check: Earthmover-managed or bring your own bucket |

### Collaboration & Access Control

Arraylake adds team-oriented security and identity management on top of Icechunk's storage layer.

| Feature | Icechunk | Arraylake |
|---|---|---|
| [Role-based access control (RBAC)](https://docs.earthmover.io/concepts/roles-and-permissions) | :material-minus: Relies on cloud IAM | :material-check: Org-level and repo-level roles |
| [SSO / SAML authentication](https://docs.earthmover.io/setup/org-access) | :material-minus: | :material-check: Google, GitHub, Microsoft AD |
| [Credential vending](https://docs.earthmover.io/setup/manage-storage) | :material-minus: You manage credentials | :material-check: Automatic temporary credential delegation |
| [API keys for service accounts](https://docs.earthmover.io/concepts/roles-and-permissions) | :material-minus: | :material-check: Scoped permissions with expiration |
| [Virtual chunk security](https://docs.earthmover.io/guide/06-virtual-datasets) | :material-minus: Every reader must manage credentials for external data sources | :material-check: Org-level policies control which external sources are accessible; readers never handle credentials |

### Data Catalog & Sharing

Arraylake provides a central catalog for scientific data with native understanding of multidimensional arrays -- making it easy to discover, explore, and share datasets within and across organizations.

| Feature | Icechunk | Arraylake |
|---|---|---|
| [Repository catalog & web UI](https://docs.earthmover.io/guide/02-manage-repos) | :material-minus: | :material-check: Browse, search, and inspect repos |
| Repository metadata & tagging | :material-minus: | :material-check: Classify and filter repos with arbitrary metadata |
| Organization-level dashboards | :material-minus: | :material-check: Aggregated view across all repos |
| [Cross-organization sharing](https://docs.earthmover.io/marketplace) | :material-minus: | :material-check: Share datasets between organizations with read-only mirrors |
| [Data marketplace](https://docs.earthmover.io/marketplace) | :material-minus: | :material-check: Publish and subscribe to datasets (free or paid) |
| [Filtered subscriptions](https://docs.earthmover.io/marketplace/data-providers) | :material-minus: | :material-check: Data providers can gate access to subsets of a dataset behind a paywall |

### Data Delivery

Arraylake's [Flux](https://docs.earthmover.io/flux) service exposes your data through industry-standard protocols, with no additional infrastructure to manage.

| Feature | Icechunk | Arraylake |
|---|---|---|
| [EDR (Environmental Data Retrieval)](https://docs.earthmover.io/flux/edr) | :material-minus: | :material-check: OGC-compliant |
| [Map Tiles API](https://docs.earthmover.io/flux/tiles) | :material-minus: | :material-check: OGC Tiles |
| [WMS (Web Map Service)](https://docs.earthmover.io/flux/wms) | :material-minus: | :material-check: OGC v1.3.0 + ncWMS extensions |
| [OPeNDAP / DAP2](https://docs.earthmover.io/flux/dap2) | :material-minus: | :material-check: |

### Operations & Monitoring

Arraylake automates routine maintenance and gives visibility into repository health.

| Feature | Icechunk | Arraylake |
|---|---|---|
| [Garbage collection & data expiration](https://docs.earthmover.io/guide/04-garbage-collection) | :material-check: You run it | :material-check: Scheduled, runs on managed compute |
| Monitoring & metrics dashboards | :material-minus: | :material-check: Repo-level and org-level |
| [Webhooks & Slack notifications](https://docs.earthmover.io/guide/07-notifications) | :material-minus: | :material-check: Commit events |
| [Performance tuning](https://docs.earthmover.io/guide/05-performance) | :material-check: Manual configuration | :material-check: `arraylake repo tune` benchmarking |

### Support & Pricing

| Feature | Icechunk | Arraylake |
|---|---|---|
| Pricing | Free forever (Apache 2.0) | Free tier (read-only) + [Professional tier](https://docs.earthmover.io/pricing) |
| Support | Community (GitHub, Slack) | Priority support |

## When to Use Which

**Use Icechunk on its own** if you are comfortable managing your own cloud infrastructure, don't need a web UI or access control beyond cloud IAM, and want full control with zero cost and zero vendor dependency.

**Use Arraylake** if you need team collaboration with role-based access, want a web UI for managing repositories, need to serve data via standard protocols (OGC, OPeNDAP), or want managed operations like garbage collection, credential vending, and monitoring.

## No Lock-in

Arraylake stores your data in Icechunk format in your own object storage (bring your own bucket), following the open Icechunk Format Specification.

## Links

- [Arraylake documentation](https://docs.earthmover.io)
- [Get started with Icechunk](getting-started/quickstart.md)
