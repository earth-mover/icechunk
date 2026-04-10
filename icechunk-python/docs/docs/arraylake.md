---
title: Arraylake
---

# Arraylake

[Icechunk](https://icechunk.io) is a free, open-source (Apache 2.0) transactional storage engine for Zarr.
[Arraylake](https://docs.earthmover.io) is Earthmover's managed cloud platform, built on top of Icechunk.
Both are made by [Earthmover](https://earthmover.io/).

Icechunk is a fully capable, standalone product -- you never need Arraylake to use it.
Arraylake adds operational, collaboration, and data delivery features on top of Icechunk for teams that want a managed experience.

## Feature Comparison

### Core Data Engine

These capabilities are available in both Icechunk and Arraylake.

| Feature | Icechunk | Arraylake |
|---|---|---|
| Transactional storage engine | :material-check: | :material-check: Built on Icechunk |
| Version control (branches, tags, time travel) | :material-check: | :material-check: |
| ACID transactions with serializable isolation | :material-check: | :material-check: |
| Virtual chunk references (HDF5, NetCDF, GRIB, TIFF) | :material-check: | :material-check: |
| Parallel / distributed writes | :material-check: | :material-check: |
| Cloud storage backends (S3, GCS, Azure, R2, etc.) | :material-check: Self-configured | :material-check: Managed or bring your own bucket |
| Local filesystem storage | :material-check: | :material-minus: |

### Collaboration & Access Control

| Feature | Icechunk | Arraylake |
|---|---|---|
| Repository catalog & web UI | :material-minus: | :material-check: Browse, search, and inspect repos |
| Role-based access control (RBAC) | :material-minus: Relies on cloud IAM | :material-check: Org-level and repo-level roles |
| SSO / SAML authentication | :material-minus: | :material-check: Google, GitHub, Microsoft AD |
| Credential vending | :material-minus: You manage credentials | :material-check: Automatic temporary credential delegation |
| API keys for service accounts | :material-minus: | :material-check: Scoped permissions with expiration |

### Data Delivery

Arraylake's [Flux](https://docs.earthmover.io/guides/flux/) service exposes your data through industry-standard protocols, with no additional infrastructure to manage.

| Feature | Icechunk | Arraylake |
|---|---|---|
| EDR (Environmental Data Retrieval) | :material-minus: | :material-check: OGC-compliant |
| Map Tiles API | :material-minus: | :material-check: OGC Tiles |
| WMS (Web Map Service) | :material-minus: | :material-check: OGC v1.3.0 + ncWMS extensions |
| OPeNDAP / DAP2 | :material-minus: | :material-check: |

### Operations & Monitoring

| Feature | Icechunk | Arraylake |
|---|---|---|
| Garbage collection & data expiration | :material-check: You run it | :material-check: Scheduled, runs on managed compute |
| Monitoring & metrics dashboards | :material-minus: | :material-check: Repo-level and org-level |
| Webhooks & Slack notifications | :material-minus: | :material-check: Commit events |
| Performance tuning | :material-check: Manual configuration | :material-check: `arraylake repo tune` benchmarking |

### Data Sharing

| Feature | Icechunk | Arraylake |
|---|---|---|
| Data marketplace | :material-minus: | :material-check: Publish and subscribe to datasets (free or paid) |

### Integrations

| Feature | Icechunk | Arraylake |
|---|---|---|
| Zarr / Xarray / Dask | :material-check: | :material-check: |
| MLflow | :material-minus: | :material-check: |
| Weights & Biases | :material-minus: | :material-check: |

### Support & Pricing

| Feature | Icechunk | Arraylake |
|---|---|---|
| Pricing | Free forever (Apache 2.0) | Free tier (read-only) + Professional tier |
| Support | Community (GitHub, Slack) | Priority support |

## When to Use Which

**Use Icechunk on its own** if you are comfortable managing your own cloud infrastructure, don't need a web UI or access control beyond cloud IAM, and want full control with zero cost and zero vendor dependency.

**Use Arraylake** if you need team collaboration with role-based access, want a web UI for managing repositories, need to serve data via standard protocols (OGC, OPeNDAP), or want managed operations like garbage collection, credential vending, and monitoring.

## No Lock-in

Arraylake stores your data in Icechunk format in your own object storage (bring your own bucket).
You can always read your data directly with the open-source Icechunk library, with no proprietary format or vendor lock-in.

## Links

- [Arraylake documentation](https://docs.earthmover.io)
- [Get started with Icechunk](getting-started/quickstart.md)
