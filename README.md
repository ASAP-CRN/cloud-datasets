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
<dataset-name>/
├── dataset.json                           # Canonical metadata (see schema below)
├── DOI/                                   # Zenodo deposition files for current version
│   ├── <dataset-name>.json                # Dataset summary
│   ├── project.json                       # Project-level details
│   ├── dataset.doi                        # Concept DOI (all versions)
│   ├── version.doi                        # Version-specific DOI
│   ├── deposition.json                    # Zenodo deposition record
│   ├── <dataset-name>_README.md
│   └── <dataset-name>_README.pdf
├── refs/                                  # Reference files for current version
├── scripts/                               # Scripts for current version
└── archive/                               # Immutable snapshots of past versions
    └── <version>/
        ├── DOI/                           # Version-specific DOI files (same structure as above)
        ├── refs/
        └── scripts/
```

## Dataset Metadata Schema

```json
{
  "name": "hafler-pmdbs-sn-rnaseq-pfc",
  "title": "team-hafler-pmdbs-sn-rnaseq-pfc",
  "description": "pmdbs-sc-rnaseq dataset from team-hafler",
  "version": "v1.0",
  "doi": "10.5281/zenodo.15490150",
  "creators": [
    {
      "name": "team-hafler",
      "affiliation": "ASAP CRN"
    }
  ],
  "keywords": ["pmdbs-sc-rnaseq", "hafler"],
  "license": "CC-BY-4.0",
  "references": [],
  "collection": "pmdbs-sc-rnaseq",
  "buckets": {
    "raw":  "gs://asap-raw-team-hafler-pmdbs-sn-rnaseq-pfc",
    "dev":  "gs://asap-dev-team-hafler-pmdbs-sn-rnaseq-pfc",
    "uat":  "gs://asap-uat-team-hafler-pmdbs-sn-rnaseq-pfc",
    "prod": "gs://asap-curated-team-hafler-pmdbs-sn-rnaseq-pfc"
  },
  "cde_version": "v3.3",
  "releases": {
    "v1.0.0": { "cde_version": "v2.1", "date": null },
    "v2.0.0": { "cde_version": "v3.0", "date": null },
    "v3.0.0": { "cde_version": "v3.2", "date": null },
    "v4.0.0": { "cde_version": "v3.3", "date": null }
  }
}
```

### Key Fields

- **`buckets`**: GCS bucket paths for each pipeline environment (`raw` → `dev` → `uat` → `prod`/curated)
- **`collection`**: The collection this dataset belongs to (or `"other"` for uncurated datasets)
- **`cde_version`**: The Common Data Elements version currently applied to this dataset
- **`releases`**: Map of release versions this dataset has appeared in, with the CDE version applied at that release

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
