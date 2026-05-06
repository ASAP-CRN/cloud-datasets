# %% [markdown]
# ASAP CRN Metadata validation
#
# # Team Alessi. ASAP CRN Metadata validation
# 18 June 2025
# Andy Henrie
##  This script is included for full provenance of platformed metadta for v3.0.0 release.
# DO NOT EXECUTE

# STEPS
# 1. metadata QC  scripts/QC_alessi_mouse_sc_rnaseq_dorsal_striatum.py (this script)
#    a. QC

# 2. prep release scripts/prep_alessi_mouse_sc_rnaseq_dorsal_striatum.py 
#    a. extract DOI info
#    b. create DOI_readme.txt
#    c. mint DOI 
#    d. create ASAP IDs for the dataset 
#    e. create "release" metadata package

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
# ### Starting with v3.1 table
team = "alessi"
dataset_name = "sn-rnaseq-dorsal-striatum-g2019s"
source = "mouse"
intake_schema_version = "v3.1"
spatial = False

metadata_path = datasets_path / f"{team}-{source}-{dataset_name}/metadata"
og_path = metadata_path / "og"  # in this contxt og is for the v2 additions

metadata_version = intake_schema_version
METADATA_VERSION_DATE = f"{metadata_version}_{pd.Timestamp.now().strftime('%Y%m%d')}"

# %%\# %%
schema_version = intake_schema_version
CDEv3 = read_CDE("v3.1")

CDE = CDEv3
tables = CDE["Table"].unique()

tables = MOUSE_TABLES if source == "mouse" else PMDBS_TABLES
tables = tables + ["SPATIAL"] if spatial else tables

# %%
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


# %%
MOUSE = dfs["MOUSE"].copy()
SAMPLE = dfs["SAMPLE"].copy()
DATA = dfs["DATA"].copy()
ASSAY_RNAseq = dfs["ASSAY_RNAseq"].copy()
CONDITION = dfs["CONDITION"].copy()
STUDY = dfs["STUDY"].copy()
PROTOCOL = dfs["PROTOCOL"].copy()

# %%
# replace spaces and apend biederer+strain to subject_id
new_subject_id = (
    "alessi_"
    + MOUSE["strain"].str.replace("/", "-").str.replace("_(RRID:IMSR_TAC:13940)", "")
    + "_"
    + MOUSE["subject_id"].str.replace(" ", "_")
)

subject_id_mapper = dict(zip(MOUSE["subject_id"], new_subject_id))

MOUSE["subject_id"] = MOUSE["subject_id"].map(subject_id_mapper)

# %%
# Process MOUSE which replaces SUBJECT table
MOUSE_, MOUSE_aux, _ = process_table(MOUSE, "MOUSE", CDE)
# %%

# %%
# Process SAMPLE table'
# add biederer+strain to subject_id
SAMPLE["subject_id"] = SAMPLE["subject_id"].map(subject_id_mapper)

# %%
# strip the trailing two file codes to get sensible sample_id
SAMPLE["sample_id"] = SAMPLE["sample_id"].apply(lambda x: "_".join(x.split("_")[:-3]))

# %%
SAMPLE["batch"] = "batch" + SAMPLE["batch"]
# also fix replicate_count, repeated_sample

SAMPLE["replicate"] = SAMPLE["replicate"].str.capitalize()

# %%
#####################################
# Process DATA table
# build from the md5_report
md5_path = datasets_path / f"{team}-{source}-{dataset_name}/file_metadata/md5_report.txt"

with open(md5_path, "r") as f:
    lines = f.readlines()
    
md5_report = {}
for line in lines:
    md5 = line.split("   ")[0].strip()
    file_name = line.split("   ")[1].strip()
    md5_report[file_name] = md5
   
md5_df = pd.DataFrame(md5_report.items(), columns=["file_name", "file_MD5"])

# %%
DATA["sample_id"] = DATA["sample_id"].apply(lambda x: "_".join(x.split("_")[:-3]))
DATA["file_name"] = DATA["file_name"].apply(lambda x: f"{x}.fastq.gz")

# %%
DATA["file_MD5"] = DATA["file_name"].map(md5_report)

# %%
DATA["batch"] = "batch" + DATA["batch"]
DATA["replicate"] = DATA["replicate"].str.capitalize()
DATA["file_type"] = "fastq"
DATA["adjustment"] = "Raw"
# %%
TMP = DATA.copy()
for sample in TMP["sample_id"].unique():
    samp_idx = TMP["sample_id"]==sample
    DATA_samp = TMP[samp_idx]
    replicate_count = DATA_samp["replicate"].nunique()
    for ii,sample_batch in enumerate(DATA_samp["replicate"].unique()):
        DATA_samp.loc[DATA_samp["replicate"]==sample_batch,"replicate_count"] = replicate_count
        DATA_samp.loc[DATA_samp["replicate"]==sample_batch,"repeated_sample"] = "0" if ii==0 else "1"

    TMP[samp_idx] = DATA_samp
TMP


# %%
DATA_, DATA_aux, _ = process_table(TMP, "DATA", CDE)



# %%
TMP = SAMPLE.drop_duplicates().copy()
# %%

for sample in TMP["sample_id"].unique():
    samp_idx = TMP["sample_id"]==sample
    SAMP_samp = TMP[samp_idx]
    replicate_count = SAMP_samp["replicate"].nunique()
    for ii,sample_batch in enumerate(SAMP_samp["replicate"].unique()):
        SAMP_samp.loc[SAMP_samp["replicate"]==sample_batch,"replicate_count"] = replicate_count
        SAMP_samp.loc[SAMP_samp["replicate"]==sample_batch,"repeated_sample"] = "0" if ii==0 else "1"

    TMP[samp_idx] = SAMP_samp
TMP
# %%
SAMPLE_, SAMPLE_aux, _ = process_table(TMP, "SAMPLE", CDE)


# %%
# ASSAY is missing... construct
# %%

############################
# build ASSAY_RNAseq from DATA
ASSAY_RNAseq["input_cell_count_"] = ASSAY_RNAseq["input_cell_count"]
ASSAY_RNAseq["input_cell_count"] = 10_000
ASSAY_RNAseq["sequencing_length"] = 150
ASSAY_RNAseq["sequencing_instrument"] = "Illumina NovaSeq 6000"

# %%
ASSAY_RNAseq_, ASSAY_RNAseq_aux, _ = process_table(ASSAY_RNAseq, "ASSAY_RNAseq", CDE)

# %%
# %% [markdown]
# these are all taken from the contribution intake form
STUDY = dfs["STUDY"].copy()
STUDY['metadata_tables'] = f"{tables}"
STUDY_, STUDY_aux, _ = process_table(STUDY, "STUDY", CDE)

# %%
PROTOCOL = dfs["PROTOCOL"].copy()
PROTOCOL_, PROTOCOL_aux, _ = process_table(PROTOCOL, "PROTOCOL", CDE)

# %% 
# CONDITION
# construct intervention.csv
intervention_df = CONDITION[["condition_id", "intervention_id"]]
intervention_df["intervention_description"] = CONDITION["intervention_name"]
intervention_df["subject_ids"] = CONDITION["intervention_aux_table"]

CONDITION["intervention_name"] = [ "Littermate wild type control", "LRRK2-G2019S homozygous knock-in mice" ]
CONDITION["intervention_aux_table"] = "intervention.csv"


CONDITION_, CONDITION_aux_, _ = process_table(CONDITION, "CONDITION", CDE)

# %%

# %% 
## now export ['STUDY', 'PROTOCOL', 'SUBJECT', 'SAMPLE', 'ASSAY_RNAseq', 'DATA', 'PMDBS', 'CLINPATH', 'CONDITION']

STUDY_.to_csv(metadata_path / "STUDY.csv", index=False)
PROTOCOL_.to_csv(metadata_path / "PROTOCOL.csv", index=False)
# SUBJECT_.to_csv(metadata_path / "SUBJECT.csv", index=False)
MOUSE_.to_csv(metadata_path / "MOUSE.csv", index=False)
SAMPLE_.to_csv(metadata_path / "SAMPLE.csv", index=False)
ASSAY_RNAseq_.to_csv(metadata_path / "ASSAY_RNAseq.csv", index=False)
DATA_.to_csv(metadata_path / "DATA.csv", index=False)
CONDITION_.to_csv(metadata_path / "CONDITION.csv", index=False)

intervention_df.to_csv(metadata_path / "intervention.csv", index=False)


# %%
# create version package
schema_version = 'v3.1'
version_metadata = metadata_path / schema_version

dfs3_1 = load_tables(metadata_path, tables)

export_meta_tables(dfs3_1, metadata_path)
create_metadata_package(dfs3_1, metadata_path, schema_version)
# %%
# copy intervention.csv
import shutil
shutil.copy(metadata_path / "intervention.csv", version_metadata / "intervention.csv")

# %%

from crn_utils.update_schema import update_tables_v3_1tov3_2

input_schema = 'v3.1'
schema_version = 'v3.2'
CDEv3_2 = read_CDE(schema_version)

input_metadata = metadata_path / input_schema
output_metadata = metadata_path / schema_version

dfs3_1 = load_tables(input_metadata, tables)


# %%
dfs3_2 = update_tables_v3_1tov3_2(dfs3_1, CDEv3_2)
create_metadata_package(dfs3_1, metadata_path, schema_version)


shutil.copy(input_metadata / "intervention.csv", output_metadata / "intervention.csv")


# %%

input_schema = 'v3.2'
schema_version = 'v3.3'
CDEv3_3 = read_CDE(schema_version)

input_metadata = metadata_path / input_schema
output_metadata = metadata_path / schema_version

dfs3_2 = load_tables(input_metadata, tables)


# %%
create_metadata_package(dfs3_2, metadata_path, schema_version)


shutil.copy(input_metadata / "intervention.csv", output_metadata / "intervention.csv")


# %%