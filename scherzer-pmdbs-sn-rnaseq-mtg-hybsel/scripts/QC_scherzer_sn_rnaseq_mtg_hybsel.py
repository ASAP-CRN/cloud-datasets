
# # Team Scherzer. ASAP CRN Metadata validation
# 26 September 2025
# Andy Henrie
##  This script is included for full provenance of platformed metadta to current 3.0.1 release.
# this updates the dataset to v1.1 with newly contributed metadata which has been harmonized across all related Scherzer datasets
# DO NOT EXECUTE
# JAH: 12-15-2025 update
# Andy Henrie
# DO NOT EXECUTE


# STEPS
# 1. metadata QC  scripts/QC_scherzer_sn_rnaseq_mtg_hybsel.py
#    a. QC
#    b. extract DOI info

# 2. prep release scripts/prep_scherzer_sn_rnaseq_mtg_hybsel_v1.1.py 
#    a. extract DOI info
#    b. create DOI_readme.txt
#    c. mint DOI 
#    d. create ASAP IDs for the dataset 
#    e. create "release" metadata package
# %%
import pandas as pd
from pathlib import Path
import os, sys

from crn_utils.util import (
    read_CDE,
    NULL,
    prep_table,
    read_meta_table,
    read_CDE_asap_ids,
    export_meta_tables,
    load_tables,
    write_version,
)
from crn_utils.asap_ids import *
from crn_utils.validate import validate_table, ReportCollector, process_table
from crn_utils.release_util import create_metadata_package, prep_release_metadata

from crn_utils.constants import *
from crn_utils.doi import *

%load_ext autoreload
%autoreload 2

# %%
root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata")
datasets_path = root_path / "datasets"

# %%
team = "scherzer"
dataset_name = "sn-rnaseq-mtg-hybsel"
source = "pmdbs"
intake_schema_version = "v3.2"
spatial = False

metadata_path = datasets_path / f"{team}-{source}-{dataset_name}/metadata"
og_path = metadata_path / "og"  # in this contxt og is for the v2 additions

metadata_version = intake_schema_version
# METADATA_VERSION_DATE = f"{metadata_version}_{20250301}"
METADATA_VERSION_DATE = f"{metadata_version}_{pd.Timestamp.now().strftime('%Y%m%d')}"

# %%\# %%
schema_version = intake_schema_version
CDE = read_CDE(schema_version)

tables = MOUSE_TABLES if source == "mouse" else PMDBS_TABLES
tables = tables + ["SPATIAL"] if spatial else tables

########################################################
##
##   BASIC METADATA QC
##
########################################################
# first load tables and
dfs = {}

for table in tables:
    table_path = og_path / f"{table}.csv"
    schema = CDE[CDE["Table"] == table]
    if table_path.exists():
        df = read_meta_table(table_path)
        report = ReportCollector(destination="NA")
        full_table, report = validate_table(df.copy(), table, schema, report)
        report.print_log()
        dfs[table] = full_table
    else:
        print(f"{table} table not found.  need to construct")
        df = pd.DataFrame(columns=schema["Field"])
        dfs[table] = df


# %%
# load our partial tables
ASSAY_RNAseq = dfs["ASSAY_RNAseq"].copy()
CONDITION = dfs["CONDITION"].copy()
DATA = dfs["DATA"].copy()
PROTOCOL = dfs["PROTOCOL"].copy()
SAMPLE = dfs["SAMPLE"].copy()
STUDY = dfs["STUDY"].copy()
SUBJECT = dfs["SUBJECT"].copy()
PMDBS = dfs["PMDBS"].copy()
CLINPATH = dfs["CLINPATH"].copy()

#############
##. need to fix the sample_ids to match the original encoding.
# set sample_id = subject_id + "_hybsel"
SAMPLE["new_sample_id"] = SAMPLE["sample_id"]
SAMPLE["sample_id"] = SAMPLE["subject_id"] + "_hybsel" 
sample_id_mapper = dict(zip(SAMPLE["new_sample_id"], SAMPLE["sample_id"]))
# %%
# fix condition_id
condition_id_mapper = {"HC": "Control", "PD": "PD","ILB": "Prodromal"}
SAMPLE["condition_id"] = SAMPLE["condition_id"].map(condition_id_mapper)

samp_condition_mapper = dict(zip(SAMPLE["subject_id"], SAMPLE["condition_id"]))

# %%
# Process SAMPLE table'
SAMPLE_, SAMPLE_aux, _ = process_table(SAMPLE, "SAMPLE", CDE)

# %%
# Process ASSAY_RNAseq table
ASSAY_RNAseq["sample_id"] = ASSAY_RNAseq["sample_id"].map(sample_id_mapper)
ASSAY_RNAseq_, ASSAY_RNAseq_aux, _ = process_table(ASSAY_RNAseq, "ASSAY_RNAseq", CDE)

# %%
# Process SUBJECT table
# JAH: 12-15-2025 update
# og_mapper = dict(zip(SUBJECT_["subject_id"], SUBJECT_["gp2_phenotype"]))
SUBJECT["gp2_phenotype"] = SUBJECT["subject_id"].map(samp_condition_mapper)
new_mapper_samp = dict(zip(SAMPLE_["subject_id"], SAMPLE_["condition_id"]))

SUBJECT_, SUBJECT_aux, _ = process_table(SUBJECT, "SUBJECT", CDE)

# %%
# # fix 'Parkinson?s disease', 'Parkinson?s disease with dementia'
# CLINPATH['path_autopsy_dx_main'] = CLINPATH['path_autopsy_dx_main'].str.replace("?", "'")

# Process CLINPATH table
CLINPATH_, CLINPATH_aux, _ = process_table(CLINPATH, "CLINPATH", CDE)


# %%
# Process DATA table
DATA["sample_id"] = DATA["sample_id"].map(sample_id_mapper)
DATA["replicate"] = "Rep1"
DATA_, DATA_aux, _ = process_table(DATA, "DATA", CDE)

# %%
# Process PMDBS table'
PMDBS["sample_id"] = PMDBS["sample_id"].map(sample_id_mapper)
PMDBS_, PMDBS_aux, _ = process_table(PMDBS, "PMDBS", CDE)


# %% 
# these are all taken from the contribution intake form
STUDY_, STUDY_aux, _ = process_table(STUDY, "STUDY", CDE)

# %%
PROTOCOL_, PROTOCOL_aux, _ = process_table(PROTOCOL, "PROTOCOL", CDE)

# %% 
# CONDITION
CONDITION["condition_id"] = CONDITION["condition_id"].map(condition_id_mapper)
CONDITION["protocol_id"] = NULL

CONDITION_, CONDITION_aux_, _ = process_table(CONDITION, "CONDITION", CDE)

# %% [markdown]
## now export ['STUDY', 'PROTOCOL', 'SUBJECT', 'SAMPLE', 'ASSAY_RNAseq', 'DATA', 'PMDBS', 'CLINPATH', 'CONDITION']

STUDY_.to_csv(metadata_path / "STUDY.csv", index=False)
PROTOCOL_.to_csv(metadata_path / "PROTOCOL.csv", index=False)
SUBJECT_.to_csv(metadata_path / "SUBJECT.csv", index=False)
SAMPLE_.to_csv(metadata_path / "SAMPLE.csv", index=False)
ASSAY_RNAseq_.to_csv(metadata_path / "ASSAY_RNAseq.csv", index=False)
DATA_.to_csv(metadata_path / "DATA.csv", index=False)
PMDBS_.to_csv(metadata_path / "PMDBS.csv", index=False)
CLINPATH_.to_csv(metadata_path / "CLINPATH.csv", index=False)
CONDITION_.to_csv(metadata_path / "CONDITION.csv", index=False)

SAMPLE_aux.to_csv(metadata_path / "SAMPLE_aux.csv", index=False)

# %%

input_schema = 'v3.2'

version_metadata = metadata_path / input_schema
dfs3_2 = load_tables(metadata_path, tables)

export_meta_tables(dfs3_2, metadata_path)

create_metadata_package(dfs3_2, metadata_path, schema_version)


####. TESTING CODE BELOW
# # %%
# long_dataset_name = f"{team}-{source}-{dataset_name}"
# ds_path = datasets_path / long_dataset_name

# map_path = root_path / "asap-ids/master"
# suffix = "ids"

# prep_release_metadata(ds_path,input_schema,map_path, suffix, spatial=spatial, source=source)

# # %%

# %%
