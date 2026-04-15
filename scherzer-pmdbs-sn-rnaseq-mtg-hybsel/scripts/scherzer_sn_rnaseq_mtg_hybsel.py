# %%  ASAP CRN Metadata validation
# # Team Scherzer. ASAP CRN Metadata validation
#
# 9 Nov 2024
# Andy Henrie
#
# ## scrnaseq_hybsel v3.1 compatible
#  This script is included for full provenance of platformed metadta to current 2.0.0 release.
# DO NOT EXECUTE
import pandas as pd
from pathlib import Path
import os, sys

# try:
#     from crn_utils.util import read_CDE, NULL, prep_table, read_meta_table, create_metadata_package
# except:
#     !pip install git+https://github.com/ASAP-CRN/crn-utils.git  # pip install  --force-reinstall --no-deps git+https://github.com/ASAP-CRN/crn-utils.git
#     from crn_utils.util import read_CDE, NULL, prep_table, read_meta_table, create_metadata_package

from crn_utils.util import (
    read_CDE,
    NULL,
    prep_table,
    read_meta_table,
    create_metadata_package,
)

# %%
from crn_utils.validate import validate_table, ReportCollector, process_table
from crn_utils.checksums import extract_md5_from_details2, get_md5_hashes
from crn_utils.bucket_util import (
    authenticate_with_service_account,
    gsutil_ls,
    gsutil_cp,
    gsutil_mv,
)

root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata/datasets")

# %% # ## CDEs
# load the relavent CDEs

schema_version = "v1"
schema_path = Path.home() / "Projects/ASAP/crn-utils/resource/CDE"
CDEv1 = read_CDE(schema_version, local_path=schema_path)
schema_version = "v2.1"
CDEv2 = read_CDE(schema_version, local_path=schema_path)
schema_version = "v3.1"
CDEv3 = read_CDE(schema_version, local_path=schema_path)

# ## Load original tables
# These were submitted as v2.1 (actually early v3.0.0 beta)
team = "scherzer"
dataset_name = "sn-rnaseq-mtg-hybsel"

# metadata_path = root_path / f"{team}/{dataset_name}/metadata"
source = "pmdbs"
metadata_path = root_path / f"{team}-{source}-{dataset_name}/metadata"
og_path = metadata_path / "og"

# %%

og_path = metadata_path / "og"
v3_path = metadata_path / "v3"


# %%
# first load tables and
tables = [
    "STUDY",
    "PROTOCOL",
    "SUBJECT",
    "SAMPLE",
    "ASSAY_RNAseq",
    "DATA",
    "PMDBS",
    "CLINPATH",
    "CONDITION",
]

dfs = {}
for table in tables:
    df = read_meta_table(v3_path / f"{table}.csv")
    dfs[table] = df


# %%

STUDY_, STUDY_aux, _ = process_table(dfs["STUDY"], "STUDY", CDEv3)
PROTOCOL_, PROTOCOL_aux, _ = process_table(dfs["PROTOCOL"], "PROTOCOL", CDEv3)


# Process SUBJECT table
SUBJECT_, SUBJECT_aux, _ = process_table(dfs["SUBJECT"], "SUBJECT", CDEv3)

# Process SAMPLE table
SAMPLE_, SAMPLE_aux, _ = process_table(dfs["SAMPLE"], "SAMPLE", CDEv3)

# Process ASSAY_RNAseq table
ASSAY_RNAseq_, ASSAY_RNAseq_aux, _ = process_table(
    dfs["ASSAY_RNAseq"], "ASSAY_RNAseq", CDEv3
)

# Process DATA table
DATA_, DATA_aux, _ = process_table(dfs["DATA"], "DATA", CDEv3)

# Process PMDBS table
PMDBS_, PMDBS_aux, _ = process_table(dfs["PMDBS"], "PMDBS", CDEv3)

# Process CLINPATH table
CLINPATH_, CLINPATH_aux, _ = process_table(dfs["CLINPATH"], "CLINPATH", CDEv3)

# Process CONDITION table
CONDITION_, CONDITION_aux, _ = process_table(dfs["CONDITION"], "CONDITION", CDEv3)

# %%


# need to
#   - add 'gp2_phenotype' to the SUBJECT
#   - add 'dataset_title', 'dataset_description', 'dataset_name' to the STUDY
#  GP2 aligned diagnosis phenotype of the subject. PD - parkinsons disease, Control - no NDD diagnosis,
#                Prodromal - prodromal parkinsons, Other - encompasses anything not in the above list for example diagnoses llike huntingtons,
#       Not reported - missing, MSA - multiple system atrophy, PSP - progressive supranuclear palsy, DLB - dementia with lewey bodies,
#               CBS - corticobasal degeneration, AD - alzheimers disease
gp2_phenotype_mapper = {
    "No PD nor other neurological disorder": "Control",
    "Healthy Control": "Control",
    "Idiopathic PD": "PD",
    "PD": "PD",
    "Prodromal motor PD": "Prodromal",
    "Prodromal Motor PD": "Prodromal",
}
SUBJECT_["gp2_phenotype"] = SUBJECT_["primary_diagnosis"].map(gp2_phenotype_mapper)


# %%

# construct this table.  needs to be checked by hand
CONDITIONv3 = pd.DataFrame(columns=CDEv3[CDEv3["Table"] == "CONDITION"]["Field"])
CONDITIONv3["condition_id"] = SUBJECT_["gp2_phenotype"].unique()
CONDITIONv3 = CONDITIONv3.fillna(NULL)
CONDITIONv3["intervention_name"] = "Case-Control"


def interventioner(x):
    if x == "Control":
        return "Control"
    elif x == "PD":
        return "Case"
    else:
        return "Other"


CONDITIONv3["intervention_id"] = CONDITIONv3["condition_id"].apply(interventioner)

# %%
# fix the condition_id
subject_id = SUBJECT_["subject_id"]
primary_diagnosis = SUBJECT_["gp2_phenotype"]

diagnosis_mapper = dict(zip(subject_id, primary_diagnosis))
SAMPLE_["condition_id"] = SAMPLE_["subject_id"].map(diagnosis_mapper)


# %% [markdown]
# ARCHIVE TO the base metatada
STUDY_.to_csv(metadata_path / "STUDY.csv", index=False)
PROTOCOL_.to_csv(metadata_path / "PROTOCOL.csv", index=False)
SUBJECT_.to_csv(metadata_path / "SUBJECT.csv", index=False)
SAMPLE_.to_csv(metadata_path / "SAMPLE.csv", index=False)
ASSAY_RNAseq_.to_csv(metadata_path / "ASSAY_RNAseq.csv", index=False)
DATA_.to_csv(metadata_path / "DATA.csv", index=False)
PMDBS_.to_csv(metadata_path / "PMDBS.csv", index=False)
CLINPATH_.to_csv(metadata_path / "CLINPATH.csv", index=False)
CONDITIONv3.to_csv(metadata_path / "CONDITION.csv", index=False)

# %% [markdown]
# now archive the auxiliary tables
if len(SAMPLE_aux.columns) > 0:
    SAMPLE_aux.to_csv(metadata_path / "SAMPLE_aux.csv", index=False)
if len(DATA_aux.columns) > 0:
    DATA_aux.to_csv(metadata_path / "DATA_aux.csv", index=False)
if len(SUBJECT_aux.columns) > 0:
    SUBJECT_aux.to_csv(metadata_path / "SUBJECT_aux.csv", index=False)
if len(CLINPATH_aux.columns) > 0:
    CLINPATH_aux.to_csv(metadata_path / "CLINPATH_aux.csv", index=False)
if len(ASSAY_RNAseq_aux.columns) > 0:
    ASSAY_RNAseq_aux.to_csv(metadata_path / "ASSAY_RNAseq_aux.csv", index=False)
if len(STUDY_aux.columns) > 0:
    STUDY_aux.to_csv(metadata_path / "STUDY_aux.csv", index=False)
if len(PROTOCOL_aux.columns) > 0:
    PROTOCOL_aux.to_csv(metadata_path / "PROTOCOL_aux.csv", index=False)


# %%  -------------------------
# ## check md5s
#
#

# %%
source = "pmdbs"
bucket = f"asap-raw-team-{team}-{source}-{dataset_name}"
# bucket = f"asap-raw-data-team-{team}" # for now old locations
key_file_path = Path.home() / f"Projects/ASAP/{team}-credentials.json"

res = authenticate_with_service_account(key_file_path)

# make sure to get ALL the fastq files in the bucket
prefix = "fastqs/*.gz"
bucket_files_md5 = get_md5_hashes(bucket, prefix)

checksum = v3_tables["DATA"][["sample_id", "file_name", "file_MD5"]]
checksum["check2"] = checksum["file_name"].map(bucket_files_md5)
checksum["check1"] = checksum["file_MD5"]
checksum[checksum.check1 != checksum.check2].file_name.to_list()
# empty means success!!

# %%
# ## prep metadata in raw data bucket
#
# steps:
# - 1. archive whats there.  i.e. move to metadata/upload
# - 2. copy metadata/upload to dataset upload (upload subdir)

# # %%
# metadata_subdir = "metadata"
# bucket = f"asap-raw-team-{team}-{source}-{dataset_name}"


# current_files = gsutil_ls(bucket,metadata_subdir)
# metadata_subdir2 = "metadata/upload"
# bucket = current_files[0].split("/")[2]

# # # %%

# for file in current_files:
#     if file == "":
#         continue
#     file_nm = Path(file).name
#     is_dir = not file_nm.__contains__(".")

#     source = f"gs://{bucket}/{metadata_subdir}/{file_nm}"
#     destination = f"gs://{bucket}/{metadata_subdir2}/{file_nm}"
#     gsutil_mv(source, destination, is_dir)

# # %%

# metadata_subdir = "metadata/upload"
# current_files = gsutil_ls(bucket,metadata_subdir)

# %%
# Archive the uploaded metadata locally
#
# But the email was shared via email, so no "upload" directory exists.
#
# Will make a copy of og/"ASAP CDE v3.0.0-beta_sks_new.xlsx" to upload/ for completeness
#
# # %%
# import shutil

# upload_path = metadata_path / "upload"
# upload_path.mkdir(exist_ok=True)
# og_file_name = "ASAP CDE v3.0.0-beta_sks_new.xlsx"
# shutil.copy(og_path / og_file_name , upload_path / og_file_name)

# %%  --------------------
# ## Create metadata package
#
# This will copy the final updated to v3.0 metadata to `asap-could-processing-resources`

metadata_source = metadata_path

source = "pmdbs"
archive_root = Path.home() / "Projects/ASAP/asap-crn-metadata/datasets"
dataset_path = archive_root / f"{team}-{source}-{dataset_name}"
# bucket = f"asap-raw-data-team-{team}" # for now old locations
fnms = create_metadata_package(metadata_source, dataset_path)

# %% _____
#
# generate ASAP IDs + transfering back to raw data bucket via `asap-crn-metadata`

# %%
