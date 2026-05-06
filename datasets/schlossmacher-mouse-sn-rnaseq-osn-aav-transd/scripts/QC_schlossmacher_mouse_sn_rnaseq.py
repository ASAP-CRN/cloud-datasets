# %% [markdown]
# ASAP CRN Metadata validation
#
# # Team Schlossmacher. ASAP CRN Metadata validation
# 16 Sept 2025
# Andy Henrie
##  This script is included for full provenance of platformed metadta to current 2.0.0 release.
# DO NOT EXECUTE

# STEPS
# 1. metadata QC  scripts/QC_schlossmacher_mouse_sn_rnaseq_osn_aav_transd.py
#    a. QC
#    b. extract DOI info

# 2. prep release scripts/prep_schlossmacher_mouse_sn_rnaseq_osn_aav_transd.py 
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
root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata/datasets")
datasets_path = root_path / "datasets"

# %%
# ### Starting with v3.2 table
team = "schlossmacher"
dataset_name = "sn-rnaseq-osn-aav-transd"
source = "mouse"
intake_schema_version = "v3.2"
spatial = False

metadata_path = root_path / f"{team}-{source}-{dataset_name}/metadata"
og_path = metadata_path / "og"  # in this contxt og is for the v2 additions

metadata_version = intake_schema_version
METADATA_VERSION_DATE = f"{metadata_version}_{20250301}"

# %%\# %%
schema_version = intake_schema_version
CDE = read_CDE(intake_schema_version)


tables = MOUSE_TABLES if source == "mouse" else PMDBS_TABLES
tables = tables + ["SPATIAL"] if spatial else tables



# # %%
# ## convert 

# sheets = ['STUDY', 'PROTOCOL', 'ASSAY_RNAseq', 'SAMPLE', 'MOUSE', 'CONDITION', 'DATA']
# excel_path = metadata_path / "og/ASAP CDE v3.0.xlsx"

# STUDY = pd.read_excel(excel_path,sheet_name="STUDY")
# PROTOCOL = pd.read_excel(excel_path,sheet_name="PROTOCOL")
# ASSAY_RNAseq = pd.read_excel(excel_path,sheet_name="ASSAY_RNAseq")
# MOUSE = pd.read_excel(excel_path,sheet_name="MOUSE")
# CONDITION = pd.read_excel(excel_path,sheet_name="CONDITION")
# DATA = pd.read_excel(excel_path,sheet_name="DATA")
# SAMPLE = pd.read_excel(excel_path,sheet_name="SAMPLE")
# SUBJECT = pd.read_excel(excel_path,sheet_name="SUBJECT")

# # %%
# # save to csvs
# STUDY.to_csv(og_path/"STUDY.csv", index=False)
# PROTOCOL.to_csv(og_path/"PROTOCOL.csv", index=False)
# ASSAY_RNAseq.to_csv(og_path/"ASSAY_RNAseq.csv", index=False)
# MOUSE.to_csv(og_path/"MOUSE.csv", index=False)
# CONDITION.to_csv(og_path/"CONDITION.csv", index=False)
# DATA.to_csv(og_path/"DATA.csv", index=False)
# SAMPLE.to_csv(og_path/"SAMPLE.csv", index=False)
# SUBJECT.to_csv(og_path/"SUBJECT.csv", index=False)


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
table = "SUBJECT"
table_path = og_path / f"{table}.csv"
df = read_meta_table(table_path)
dfs[table] = df
# %%
# mouse + subject 
MOUSE = dfs["MOUSE"].copy()
SUBJECT = dfs["SUBJECT"].copy()

SAMPLE = dfs["SAMPLE"].copy()
DATA = dfs["DATA"].copy()
CONDITION = dfs["CONDITION"].copy()
ASSAY_RNAseq = dfs["ASSAY_RNAseq"].copy()
STUDY = dfs["STUDY"].copy()
PROTOCOL = dfs["PROTOCOL"].copy()

# %% 
# melt DATA "file_name" and "file_MD5" which has semicolon separated values
DATA = dfs["DATA"].copy()

# DATA['file_name'] = DATA['file_name'].str.split(';')
# DATA['file_MD5'] = DATA['file_MD5'].str.split(';')
# DATA_exploded = DATA.explode(['file_name', 'file_MD5'])

# load all of the .md5 files in the md5s dir and create a dataframe with file_name and file_MD5 as columns
md5s_path = metadata_path.parent / "refs/md5s"
md5_files = list(md5s_path.glob("*.md5"))
lines = {}
for md5_file in md5_files:
    with open(md5_file, "r") as f:
        line = f.readline()
        parts = line.split("  ./")
        md5 = parts[0].strip()
        file_name = parts[1].strip()
        lines[file_name] = md5



md5_df = pd.DataFrame(columns=["file_name", "file_MD5"], data=lines.items())

md5_df["sample_id"] = file_name = md5_df["file_name"].apply(lambda x: x.split("_")[0]).str.lstrip("ASAP-").str.capitalize()

md5_df["replicate"] = "Rep1"
md5_df["replicate_count"] = 1
md5_df["repeated_sample"] = 0
md5_df["batch"] = "batch" + md5_df["sample_id"].str[-1]

md5_df["file_type"] = "fastq"
md5_df["file_description"] = "Paired-end fastq files"


# %%
# Process DATA table
DATA_, DATA_aux, _ = process_table(md5_df, "DATA", CDE)


# %%
MOUSE = dfs["SUBJECT"].copy()

MOUSE["age"] = 9*7
MOUSE["sex"] = "Male"
MOUSE["strain"] = "Unknown"
# Process SUBJECT table
MOUSE_, MOUSE_aux, _ = process_table(MOUSE, "MOUSE", CDE)

# 🚨⚠️❗ **Missing Required Fields in MOUSE: sex**
# 🚨⚠️❗ **Missing Optional Fields in MOUSE: age, aux_table**
# infer Sex from SAMPLE below
# %%
# Process SAMPLE table'
# fix replicate
SAMPLE["replicate"] = "Rep1"
SAMPLE["batch"] = "batch" + SAMPLE["sample_id"].str[-1]
# %%  Sample and Subject IDs are identical in this study
SAMPLE["subject_id"] = "Mouse_AAVOSN"

SAMPLE_, SAMPLE_aux, _ = process_table(SAMPLE, "SAMPLE", CDE)


# %%
# Process ASSAY_RNAseq table
ASSAY_RNAseq["input_cell_count"] = 40_000
ASSAY_RNAseq["sequencing_end"] = "Paired-end"
ASSAY_RNAseq["sequencing_instrument"] = "Illumina NovaSeq 6000"
ASSAY_RNAseq_, ASSAY_RNAseq_aux, _ = process_table(ASSAY_RNAseq, "ASSAY_RNAseq", CDE)
# %%
# these are all taken from the contribution intake form
STUDY['metadata_tables'] = f"{tables}"

STUDY_, STUDY_aux, _ = process_table(STUDY, "STUDY", CDE)

# %%
PROTOCOL_, PROTOCOL_aux, _ = process_table(PROTOCOL, "PROTOCOL", CDE)

# %%
# need to construct CONDITION table.
CONDITION = dfs["CONDITION"]
CONDITION["condition_id"] = "Treatment"
CONDITION["intervention_name"] = "AAV-OSN"
CONDITION["intervention_id"] = "AAV-OSN"

CONDITION_, CONDITION_aux, _ = process_table(CONDITION, "CONDITION", CDE)



# %% [markdown]
## now export ['STUDY', 'PROTOCOL', 'SUBJECT', 'SAMPLE', 'ASSAY_RNAseq', 'DATA', 'PMDBS', 'CLINPATH', 'CONDITION']

STUDY_.to_csv(metadata_path / "STUDY.csv", index=False)
PROTOCOL_.to_csv(metadata_path / "PROTOCOL.csv", index=False)
MOUSE_.to_csv(metadata_path / "MOUSE.csv", index=False)
SAMPLE_.to_csv(metadata_path / "SAMPLE.csv", index=False)
ASSAY_RNAseq_.to_csv(metadata_path / "ASSAY_RNAseq.csv", index=False)
DATA_.to_csv(metadata_path / "DATA.csv", index=False)
CONDITION_.to_csv(metadata_path / "CONDITION.csv", index=False)

# %%
# %%
# create version package
schema_version = 'v3.2'
version_metadata = metadata_path / schema_version

dfs3_2 = load_tables(metadata_path, tables)

export_meta_tables(dfs3_2, metadata_path)
create_metadata_package(dfs3_2, metadata_path, schema_version)


# %%
