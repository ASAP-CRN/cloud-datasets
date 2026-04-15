# %% [markdown]
# ASAP CRN Metadata validation
#
# # Team Scherzer. ASAP CRN Metadata validation
# 27 May 2025
# Andy Henrie
##  This script is included for full provenance of platformed metadta to current 2.0.2 release.
# DO NOT EXECUTE


# STEPS
# 1. metadata QC  scripts/scherzer_spatial_rnaseq_mtg.py (this script)
#    a. QC
#    b. extract DOI info
#    c. save DOI info
#    d. ad-hoc copy of artefacts/ artefacts/scripts/
# 2. DOI   scripts/scherzer_spatial_rnaseq_mtg_DOIgen.py
#    a. mint DOI
# 3. prep release  scripts/prep_scherzer_spatial_rnaseq_mtg.py
#    a. create ASAP IDs for the dataset
#    b. create release metadata package
# TODO:  3b. add artifacts.csv + raw_files.csv

import pandas as pd
from pathlib import Path
import os

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


# %%
from crn_utils.update_schema import v1_to_v2, v2_to_v3_PMDBS, intervention_typer
from crn_utils.checksums import extract_md5_from_details2, get_md5_hashes, get_md5_hashes_full
from crn_utils.bucket_util import (
    authenticate_with_service_account,
    gsutil_ls,
    gsutil_cp,
    gsutil_mv,
)

from crn_utils.constants import *
from crn_utils.doi import *

%load_ext autoreload
%autoreload 2


# %%
root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata/datasets")
datasets_path = root_path / "datasets"


# %%
schema_path = Path.home() / "Projects/ASAP/crn-utils/resource/CDE"
schema_version = "v3.2"
# CDEv3 = read_CDE(schema_version, local_path=schema_path)

# %% [markdown]
## sanity check
CDEv3 = read_CDE("v3.2")
# filen = Path.home() / "Downloads/data (10).csv"

# CDEv3 = pd.read_csv(filen)

# %%  ## Load original tables
# These were submitted as v3.1
# ### Starting with v3.1 table
team = "scherzer"
dataset_name = "spatial-visium-mtg"

source = "pmdbs"
metadata_path = root_path / f"{team}-{source}-{dataset_name}/metadata"
og_path = metadata_path / "og"  # in this contxt og is for the v2 additions

metadata_version = "v3.2"
METADATA_VERSION_DATE = f"{metadata_version}_{pd.Timestamp.now().strftime('%Y%m%d')}"

# %%
CDE = CDEv3
tables = PMDBS_TABLES + ["SPATIAL"]


# # %%
# # first load tables and
# dfs = {}

# for table in tables:
#     table_path = v3_path / f"{table}.csv"
#     schema = CDE[CDE["Table"] == table]
#     if table_path.exists():
#         df = read_meta_table(table_path)
#         report = ReportCollector(destination="NA")
#         full_table, report = validate_table(df.copy(), table, schema, report)
#         report.print_log()
#         dfs[table] = full_table
#     else:
#         print(f"{table} table not found.  need to construct")
#         df = pd.DataFrame(columns=schema["Field"])
#         dfs[table] = df




# %%
# ASSAY is missing... construct
cde_final = read_meta_table(og_path / f"ASAP-CDE-ScherzerTeam-spatial-mtg-cde-final.csv")
CLINPATH = read_meta_table(og_path / f"ASAP-CDE-ScherzerTeam-spatial-mtg-clinpath.csv")
data_dictionary = read_meta_table(og_path / f"ASAP-CDE-ScherzerTeam-spatial-mtg-clinpath.csv")
DATA = read_meta_table(og_path / f"ASAP-CDE-ScherzerTeam-spatial-mtg-data-st.csv")
PROTOCOL = read_meta_table(og_path / f"ASAP-CDE-ScherzerTeam-spatial-mtg-protocol.csv")
SAMPLE = read_meta_table(og_path / f"ASAP-CDE-ScherzerTeam-spatial-mtg-sample-st.csv")
SUBJECT = read_meta_table(og_path / f"ASAP-CDE-ScherzerTeam-spatial-mtg-subject.csv")

# %%


# Process ASSAY_RNAseq table
ASSAY_RNAseq_, ASSAY_RNAseq_aux, _ = process_table(ASSAY_RNAseq, "ASSAY_RNAseq", CDE)



# %%
SUBJECT = dfs["SUBJECT"]
# need to add gp2_phenotype
gp2_phenotype_mapper = {
    "Healthy Control": "Control",
    "Idiopathic PD": "PD",
    "PD": "PD",
}
SUBJECT["gp2_phenotype"] = SUBJECT["primary_diagnosis"].map(gp2_phenotype_mapper)

condition_mapper = dict(zip(SUBJECT["subject_id"], SUBJECT["gp2_phenotype"]))

# Process SUBJECT table
SUBJECT_, SUBJECT_aux, _ = process_table(SUBJECT, "SUBJECT", CDE)
# %%

# %%
CLINPATH = dfs["CLINPATH"]
# Process CLINPATH table
CLINPATH_, CLINPATH_aux, _ = process_table(CLINPATH, "CLINPATH", CDE)

# %%
DATA = dfs["DATA"]# Process DATA table
DATA_, DATA_aux, _ = process_table(DATA, "DATA", CDE)

# %%
SAMPLE = dfs["SAMPLE"]
SAMPLE["organism"] = "Human"
SAMPLE['assay_type'] = "snRNAseq"

SAMPLE["condition_id"] = SAMPLE["subject_id"].map(condition_mapper)
# Process SAMPLE table
SAMPLE_, SAMPLE_aux, _ = process_table(SAMPLE, "SAMPLE", CDE)



# %%%
PMDBS = dfs["PMDBS"]
PMDBS["hemisphere"] = NULL
PMDBS_, PMDBS_aux, _ = process_table(PMDBS, "PMDBS", CDE)

# have Team fill in missing 'hemisphere'

# %%
STUDY = dfs["STUDY"]
STUDY["metadata_tables"] = f"{tables}"

# Process STUDY table
STUDY_, STUDY_aux, _ = process_table(STUDY, "STUDY", CDE)

# %%
PROTOCOL = dfs["PROTOCOL"]
PROTOCOL_, PROTOCOL_aux, _ = process_table(PROTOCOL, "PROTOCOL", CDE)


# %%


# %% [markdown]
# CONDITION
CONDITION = dfs["CONDITION"]
condition_id_mapper = {"healthy_control": "Control", "idiopathic_pd": "PD"}
CONDITION["condition_id"] = CONDITION["condition_id"].map(condition_id_mapper)
CONDITION_, CONDITION_aux, _ = process_table(CONDITION, "CONDITION", CDE)


# %%
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




# %%

dfs = {}

for table in tables:
    table_path = metadata_path / f"{table}.csv"
    schema = CDE[CDE["Table"] == table]
    df = read_meta_table(table_path)
    report = ReportCollector(destination="NA")
    full_table, report = validate_table(df.copy(), table, schema, report)
    report.print_log()
    dfs[table] = full_table


# %% [markdown]
long_dataset_name = f"{team}-{source}-{dataset_name}"
root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata")
datasets_path = root_path / "datasets"
ds_path = datasets_path / long_dataset_name
intake_doc = (
    ds_path
    / "refs/Dataset Information Template_v2 LZ.docx"
)


ingest_DOI_doc(ds_path, intake_doc, publication_date="2023-09-28")


# %%
# write the files.
make_md_file(ds_path)
make_pdf_file(ds_path)

update_study_table( ds_path )

# %%  final test
# first load tables and


dfs = {}

for table in tables:
    df = read_meta_table(metadata_path / f"{table}.csv")
    schema = CDE[CDE["Table"] == table]
    report = ReportCollector(destination="NA")
    full_table, report = validate_table(df.copy(), table, schema, report)
    report.print_log()
    dfs[table] = full_table


# %%
# final export to v3.1 (current metadat)
final_metadata_path = metadata_path / schema_version
if not final_metadata_path.exists():
    final_metadata_path.mkdir()

export_meta_tables(dfs, final_metadata_path)
export_meta_tables(dfs, metadata_path)
# %%

write_version(schema_version, metadata_path / "cde_version")
write_version(schema_version, final_metadata_path / "cde_version")

# %%



import json
from pathlib import Path
import os

from crn_utils.zenodo_util import setup_zenodo
from crn_utils.doi import *
# # %%
%load_ext dotenv
%dotenv

# %% [markdown]

api_token = setup_zenodo()
# %%

ds_path = datasets_path / long_dataset_name

metadata = setup_DOI(ds_path)

deposition, upload_response = create_DOI(ds_path, metadata, api_token)
archive_deposition_local(ds_path, "deposition", deposition)

published_deposition = finalize_DOI(ds_path, deposition, api_token)
archive_deposition_local(ds_path, "deposition", deposition)

# %%



# %%
import pandas as pd
from pathlib import Path
import os, sys

import crn_utils
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
from crn_utils.constants import *
from crn_utils.doi import update_study_table_with_doi
from crn_utils.file_metadata import gen_raw_bucket_summary, make_file_metadata, update_data_table_with_gcp_uri
from crn_utils.bucket_util import authenticate_with_service_account

%load_ext autoreload
%autoreload 2
##############
# %%

pmdbs_tables = PMDBS_TABLES


map_path = root_path / "asap-ids/master"
suffix = "ids"

retval = load_pmdbs_id_mappers(map_path, suffix)
datasetid_mapper, subjectid_mapper, sampleid_mapper, gp2id_mapper, sourceid_mapper = (
    retval
)

# %%
ds = long_dataset_name

schema_version = "v3.1"
CDE = read_CDE(schema_version)
asap_ids_df = read_CDE_asap_ids()
# asap_ids_df = read_CDE_asap_ids()
asap_ids_schema = asap_ids_df[["Table", "Field"]]


# ds_path.mkdir(parents=True, exist_ok=True)
mdata_path = ds_path / "metadata"
tables = [
    table
    for table in mdata_path.iterdir()
    if table.is_file() and table.suffix == ".csv"
]

table_names = [table.stem for table in tables if table.stem in pmdbs_tables]

dfs = load_tables(mdata_path, table_names)

retval = update_pmdbs_id_mappers(
    dfs["CLINPATH"],
    dfs["SAMPLE"],
    ds,
    datasetid_mapper,
    subjectid_mapper,
    sampleid_mapper,
    gp2id_mapper,
    sourceid_mapper,
)
(
    datasetid_mapper,
    subjectid_mapper,
    sampleid_mapper,
    gp2id_mapper,
    sourceid_mapper,
) = retval

dfs = update_pmdbs_meta_tables_with_asap_ids(
    dfs,
    ds,
    asap_ids_schema,
    datasetid_mapper,
    subjectid_mapper,
    sampleid_mapper,
    gp2id_mapper,
    sourceid_mapper,
)


#### CREATE file metadata summaries
team = ds.split("-")[0]
dataset_name = "-".join(ds.split("-")[2:])
source = ds.split("-")[1]
long_dataset_name = f"{team}-{source}-{dataset_name}"

raw_bucket_name = f"asap-raw-team-{ds}"

key_file_path = Path.home() / f"Projects/ASAP/{team}-credentials.json"
res = authenticate_with_service_account(key_file_path)

file_metadata_path = ds_path / "file_metadata"
if not file_metadata_path.exists():
    file_metadata_path.mkdir(parents=True, exist_ok=True)

gen_raw_bucket_summary(raw_bucket_name, file_metadata_path, dataset_name)

make_file_metadata(ds_path, file_metadata_path, dfs["DATA"])

# export the tables to the metadata directory in a release subdirectory
out_dir = ds_path / "metadata/release"
out_dir.mkdir(parents=True, exist_ok=True)

dfs["STUDY"] = update_study_table_with_doi(dfs["STUDY"], ds_path)
dfs["DATA"] = update_data_table_with_gcp_uri(dfs["DATA"], ds_path)

export_meta_tables(dfs, out_dir)
write_version(schema_version, out_dir / "cde_version")

# # %%
# export_map_path = root_path / "asap-ids/master"
# export_pmdbs_id_mappers(
#     export_map_path,
#     suffix,
#     datasetid_mapper,
#     subjectid_mapper,
#     sampleid_mapper,
#     gp2id_mapper,
#     sourceid_mapper,
# )




# %%
