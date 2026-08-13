# cloud-datasets

Source-of-truth archive for ASAP CRN Cloud _Datasets_. Each dataset corresponds to a team's contribution of a specific tissue type and assay modality, backed by a Zenodo DOI and linked to GCS storage buckets across pipeline environments.

This repository is automatically managed by the [cloud-orchestration](https://github.com/ASAP-CRN/cloud-orchestration) system. Manual changes should be avoided.

## Dataset Naming Convention

Datasets follow the pattern `<team>-<tissue>-<modality>`, e.g.:
- `hafler-pmdbs-sn-rnaseq-pfc`
- `cohort-mouse-sc-rnaseq`
- `jakobsson-invitro-bulk-rnaseq-dopaminergic`

## Structure

```
datasets.json                              # Master index of all datasets
WIP/                                       # Staging area for in-progress datasets
datasets/
└── <dataset-name>/
    ├── dataset.json                       # Canonical metadata (see schema below)
    ├── DOI/                               # Zenodo deposition files for current version
    │   ├── <dataset-name>.json            # Dataset summary
    │   ├── project.json                   # ingest document details
    │   ├── dataset.doi                    # Concept DOI (all versions)
    │   ├── version.doi                    # Version-specific DOI
    │   ├── deposition.json                # Zenodo deposition record
    │   ├── <dataset-name>_README.md
    │   └── <dataset-name>_README.pdf
    ├── refs/                              # Reference files for current version
    └── archive/                           # Immutable snapshots of past versions
        └── <version>/
            ├── DOI/                       # Version-specific DOI files (same structure as above)
            └── refs/
```

## Dataset Metadata Schema


TODO:  add curation_version information
- None for uncurated datasets
- release version for when "curated" outputs are added.  (e.g. v4.1.0 release dataset version / metadata bumps which did NOT change the curated outputs.)

example
```json
{
    "name": "hafler-pmdbs-sn-rnaseq-pfc",
    "title": "Single-cell transcriptomic and proteomic analysis of Parkinson\u2019s disease brains",
    "description": "To identify ...[REDACTED].",
    "version": "v1.1",
    "doi": "10.5281/zenodo.15490150",
    "creators": [
        {
            "name": "Zhang, Le",
            "affiliation": "Yale University",
            "orcid": "0000-0002-4860-831X"
        },
        {
            "name": "Sreeganga, Chandra",
            "affiliation": "Yale University",
            "orcid": "0000-0001-9035-1733"
        },
        {
            "name": "Biqing, Zhu",
            "affiliation": "Yale University",
            "orcid": "0000-0002-7428-6297"
        },
        {
            "name": "Jae Min, Park",
            "affiliation": "Yale University",
            "orcid": "0000-0002-9770-7197"
        },
        {
            "name": "Anthony, Russo",
            "affiliation": "Yale University",
            "orcid": "0000-0002-0623-6618"
        },
        {
            "name": "Haowei, Wang",
            "affiliation": "Yale University"
        }
    ],
    "keywords": [
        "pmdbs-sc-rnaseq",
        "pmdbs-sc-rnaseq",
        "hafler"
    ],
    "license": "CC-BY-4.0",
    "collection": "pmdbs-sc-rnaseq",
    "buckets": {
        "raw": "gs://asap-raw-team-hafler-pmdbs-sn-rnaseq-pfc",
        "dev": "gs://asap-dev-team-hafler-pmdbs-sn-rnaseq-pfc",
        "uat": "gs://asap-uat-team-hafler-pmdbs-sn-rnaseq-pfc",
        "prod": "gs://asap-curated-team-hafler-pmdbs-sn-rnaseq-pfc"
    },
    "cde_version": "v2.1",
    "releases": {
        "v1.0.0": {
            "cde_version": "v2.1",
            "dataset_version": "v1.0"
        },
        "v2.0.0": {
            "cde_version": "v3.0",
            "dataset_version": "v1.0"
        },
        "v3.0.0": {
            "cde_version": "v3.2",
            "dataset_version": "v1.0"
        },
        "v4.0.0": {
            "cde_version": "v3.3",
            "dataset_version": "v1.0"
        },
        "v4.1.0": {
            "cde_version": "v3.3",
            "dataset_version": "v1.1"
        }
    },
    "dataset_title": "Single-cell transcriptomic and proteomic analysis of Parkinson\u2019s disease brains",
    "curation": {
        "name": "pmdbs-sc-rnaseq",
        "dataset_version": "v1.0",
        "release_version": "v4.0.0",
        "collection_version": "v3.1.0",
        "collection": {
            "name": "pmdbs-sc-rnaseq",
            "version": "v3.1.0",
            "collection_doi": "10.5281/zenodo.14373047"
        },
        "releases": {
            "v1.0.0": {
                "collection_version": "v1.0.0",
                "release_version": "v1.0.0",
                "collection_version_doi": null
            },
            "v2.0.0": {
                "collection_version": "v2.0.0",
                "release_version": "v2.0.0",
                "collection_version_doi": null
            },
            "v3.1.0": {
                "collection_version": "v3.1.0",
                "release_version": "v4.0.0",
                "collection_version_doi": "10.5281/zenodo.17860778"
            }
        }
    },
    "all_releases": [
        "v1.0.0",
        "v2.0.0",
        "v3.0.0",
        "v4.0.0",
        "v4.1.0"
    ],
    "all_versions": [
        "v1.1",
        "v1.0"
    ],
    "short_description": "pmdbs-sc-rnaseq dataset from team-hafler"
}


```


## GCS Curated Bucket Layout

Each dataset's production (`prod`) bucket follows this layout:

```
gs://asap-curated-<dataset-name>/
├── artifacts/
├── file_metadata/
├── metadata/
│   └── release/<release_version>/
│       ├── *.csv
│       └── cde_version
└── <workflow_name>/
    └── release/<release_version>/
        ├── <curated_outputs>/
        └── workflow_version
```

The curated bucket is **mutable** — it accumulates outputs from all released versions. Versioned collection buckets (managed in `cloud-collections`) hold immutable snapshots.

## Dataset Lifecycle

1. **Acceptance** — new or updated dataset is registered with the orchestration system
2. **Scoping** — dataset is associated with a release version and a collection (if applicable)
3. **DOI assignment** — new datasets get an initial concept DOI; updated datasets get a new version DOI
4. **Release** — `dataset.json` is updated, DOI files are written, archive snapshot is created

## Management

For dataset submissions or updates, use the orchestration system or contact the ASAP CRN team.
