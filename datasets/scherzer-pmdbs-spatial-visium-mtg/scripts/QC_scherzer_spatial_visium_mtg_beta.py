# %% [markdown]
# ASAP CRN Metadata validation
#
# # Team Scherzer. ASAP CRN Metadata validation
# 27 May 2025
# Andy Henrie
##  This script is included for full provenance of platformed metadta to current 2.0.2 release.
# DO NOT EXECUTE


# STEPS
# 1. metadata QC  scripts/QC_scherzer_spatial_visium_mtg.py (this script)
#    a. QC

# 2. prep release scripts/prep_scherzer_spatial_visium_mtg.py 
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


# %%
from crn_utils.update_schema import v1_to_v2, v2_to_v3_PMDBS, intervention_typer
from crn_utils.checksums import extract_md5_from_details2, get_md5_hashes

from crn_utils.constants import *
from crn_utils.doi import *

%load_ext autoreload
%autoreload 2


# %%
root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata/datasets")
datasets_path = root_path / "datasets"
# %%  ## Load original tables
# These were submitted as v3.1_path / "datasets"

# %%
# ### Starting with v3.1 table
team = "scherzer"
dataset_name = "spatial-visium-mtg"

source = "pmdbs"
intake_schema_version = "v3.1"

metadata_path = root_path / f"{team}-{source}-{dataset_name}/metadata"
og_path = metadata_path / "og"  # in this contxt og is for the v2 additions

metadata_version = intake_schema_version
METADATA_VERSION_DATE = f"{metadata_version}_{pd.Timestamp.now().strftime('%Y%m%d')}"

# %%
schema_version = intake_schema_version
CDEv3 = read_CDE("v3.1")

CDE = CDEv3
tables = PMDBS_TABLES + ["SPATIAL"]


# %%
# ASSAY is missing... construct
# cde_final = read_meta_table(og_path / f"ASAP-CDE-ScherzerTeam-spatial-mtg-cde-final.csv")
CLINPATH = read_meta_table(og_path / f"ASAP-CDE-ScherzerTeam-spatial-mtg-clinpath.csv")
# data_dictionary = read_meta_table(og_path / f"ASAP-CDE-ScherzerTeam-spatial-mtg-clinpath.csv") # CLINPATH copy
DATA = read_meta_table(og_path / f"ASAP-CDE-ScherzerTeam-spatial-mtg-data-st.csv")
PROTOCOL = read_meta_table(og_path / f"ASAP-CDE-ScherzerTeam-spatial-mtg-protocol.csv")
SAMPLE = read_meta_table(og_path / f"ASAP-CDE-ScherzerTeam-spatial-mtg-sample-st.csv")
SUBJECT = read_meta_table(og_path / f"ASAP-CDE-ScherzerTeam-spatial-mtg-subject.csv")

CLINPATH.to_csv(og_path / f"CLINPATH.csv", index=False)
DATA.to_csv(og_path / f"DATA.csv", index=False)
PROTOCOL.to_csv(og_path / f"PROTOCOL.csv", index=False)
SAMPLE.to_csv(og_path / f"SAMPLE.csv", index=False)
SUBJECT.to_csv(og_path / f"SUBJECT.csv", index=False)

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
SUBJECT = dfs["SUBJECT"].copy()
gp2_phenotype_mapper = {
    "Healthy Control": "Control",
    "Idiopathic PD": "PD",
    "Prodromal motor PD": "Prodromal",
}
SUBJECT["gp2_phenotype"] = SUBJECT["primary_diagnosis"].map(gp2_phenotype_mapper)
condition_mapper = dict(zip(SUBJECT["subject_id"], SUBJECT["gp2_phenotype"]))

SUBJECT_, SUBJECT_aux, _ = process_table(SUBJECT, "SUBJECT", CDE)

# THESE belong in CLINPATH
# 🚨⚠️❗ **Extra field in SUBJECT: AMPPD_id**
# 🚨⚠️❗ **Extra field in SUBJECT: GP2_id**
# 🚨⚠️❗ **Extra field in SUBJECT: organism**
# 🚨⚠️❗ **Extra field in SUBJECT: ethnicity**
# 🚨⚠️❗ **Extra field in SUBJECT: family_history**
# 🚨⚠️❗ **Extra field in SUBJECT: last_diagnosis**
# 🚨⚠️❗ **Extra field in SUBJECT: age_at_onset**
# 🚨⚠️❗ **Extra field in SUBJECT: age_at_diagnosis**
# 🚨⚠️❗ **Extra field in SUBJECT: first_motor_symptom**
# 🚨⚠️❗ **Extra field in SUBJECT: hx_dementia_mci**
# 🚨⚠️❗ **Extra field in SUBJECT: hx_melanoma**
# 🚨⚠️❗ **Extra field in SUBJECT: education_level**
# 🚨⚠️❗ **Extra field in SUBJECT: smoking_status**
# 🚨⚠️❗ **Extra field in SUBJECT: smoking_years**
# 🚨⚠️❗ **Extra field in SUBJECT: APOE_e4_status**
# 🚨⚠️❗ **Extra field in SUBJECT: cognitive_status**
# 🚨⚠️❗ **Extra field in SUBJECT: time_from_baseline**
# 🚨⚠️❗ **Extra field in SUBJECT: Unnamed: 25**

# %%

# %%
CLINPATH = dfs["CLINPATH"].copy()
# Process CLINPATH table
CLINPATH_, CLINPATH_aux, _ = process_table(CLINPATH, "CLINPATH", CDE)

# 🚨⚠️❗ **7 Fields with invalid entries:**
# - _*path_autopsy_dx_main*_:  invalid values 💩'PD/Dem', 'Control', 'Control (MCI)', 'PD/ND', 'Control, Non-motoric', 'PDD', 'MCI', 'PD (MCI)', 'Control (history)', 'Control (diseased)', 'MCI (clinical history)', 'control (clinical history)'
#     - valid ➡️ 'Lewy body disease nos', 'Parkinson's disease', 'Parkinson's disease with dementia', 'Dementia with Lewy bodies', 'Multiple system atrophy (SND>OPCA)', 'Multiple system atrophy (OPCA<SND)', 'Multiple system atrophy (SND=OPCA)', 'Progressive supranuclear palsy', 'Corticobasal degeneration', 'Globular glial tauoapathy (GGT)', 'Chronic traumatic encephalopathy (CTE)', 'FTLD-Tau (Pick's)', 'FTLD-Tau (MAPT)', 'FTLD-Tau (AGD)', 'FTLD-TDP43, Type A', 'FTLD-TDP43, Type B', 'FTLD-TDP43, Type C', 'FTLD-TDP43, Type D', 'FTLD-TDP43, Type E', 'Motor neurone disease-TDP43 (MND or ALS)', 'FTLD-MND-TDP43', 'Huntington's disease', 'Spinocerebellar ataxia, nos', 'Prion disease, nos', 'Alzheimer's disease (high level neuropathological change)', 'Alzheimer's disease (intermediate level neuropathological change)', 'Control, Low level AD neuropathological change', 'Control, Limbic predominant age-related TDP43 proteinopathy (LATE)', 'Control, Argyrophilic grain disease', 'Control, Primary age-related tauopathy (PART)', 'Control, Ageing-related tau astrogliopathy (ARTAG)', 'Control, Cerebrovascular disease (atherosclerosis)', 'Control, Cerebrovascular disease (hyaline arteriolosclerosis)', 'Control, Cerebrovascular disease (cerebral amyloid angiopathy)', 'Control, no misfolded protein or significant vascular pathology', 'Control, Low level TDP-43 neuropathological change', 'Control, Low level PSP neuropathological change', 'Other neurological disorder', 'NA'
# - _*path_cerad*_:  invalid values 💩'moderate', 'sparse', 'frequent'
#     - valid ➡️ 'None', 'Sparse', 'Moderate', 'Frequent', 'NA'
# - _*path_thal*_:  invalid values 💩'Phase 0 (A0)', 'Phase 1 (A1)', 'Phase 5 (A3)', 'Phase 3 (A2)', 'Phase 2 (A1)', 'Missing/unknown', 'Phase 4 (A3)'
#     - valid ➡️ '0', '1', '2', '3', '4', '5', '1/2', '3', '4/5', 'NA'
# - _*path_mckeith*_:  invalid values 💩'llb. Limbic Predominant', 'lll. Brainstem/Limbic', '0. No Lewy bodies', 'lla. Brainstem Predominant', 'lV. Neocortical', 'l. Olfactory Bulb-Only'
#     - valid ➡️ 'Neocortical', 'Limbic (transitional)', 'Brainstem', 'Amygdala Predominant', 'Olfactory bulb only', 'Limbic, transitional (brainstem and limbic involvement)', 'Diffuse, neocortical (brainstem, limbic and neocortical involvement)', 'Olfactory Bulb-Only', 'Limbic (amygdala) predominant', 'Absent', 'Present, but extent unknown', 'NA'
# - _*sn_neuronal_loss*_:  invalid values 💩'3', '0', '1', '2'
#     - valid ➡️ 'None', 'Mild', 'Moderate', 'Severe', 'Not assessed', 'Unknown', 'NA'
# - _*path_nia_aa_a*_:  invalid values 💩'Low ADNC', 'Not AD', 'Intermediate ADNC'
#     - valid ➡️ 'A0', 'A1', 'A2', 'A3', 'NA'
# - _*TDP43*_:  invalid values 💩'no', 'undx', 'yes'
#     - valid ➡️ 'None in medial temporal lobe', 'Present in amygdala, only', 'Present in hippocampus, only', 'Present in entorhinal cortex, only', 'Present in amygdala and hippocampus, only', 'Present in the limbic region (where limbic includes amygdala, entorhinal cortex, hippocampus, and medial temporal lobe', 'Present in medial temporal lobe and middle frontal gyrus (not FTLD pattern)', 'Unknown', 'NA'
# 🚨⚠️❗ **Extra field in CLINPATH: path_thal.1**
# 🚨⚠️❗ **Extra field in CLINPATH: path_mckeith V3**
# 🚨⚠️❗ **Extra field in CLINPATH: sn_neuronal_loss V3**
# 🚨⚠️❗ **Extra field in CLINPATH: path_nia_aa_a V3**
# 🚨⚠️❗ **Extra field in CLINPATH: TDP43 V3**



# %%
DATA = dfs["DATA"].copy()# Process DATA table
DATA["replicate"] = DATA["replicate"].str.capitalize()
DATA["repeated_sample"] = DATA["repeated_sample"].map({"FALSE": 0, "TRUE": 1})
DATA["batch"] = "batch" + DATA["batch"]
DATA_, DATA_aux, _ = process_table(DATA, "DATA", CDE)

## these image files should go into the "SPATIAL" table
# - _*file_type*_:  invalid values 💩'aligned_fiducials', 'detected_tissue_image', 'scalefactors_json', 'tissue_hires_image', 'tissue_lowres_image', 'tissue_positions_list', 'tissue_hires_image_original', 'tissue_lowres_image_original'
#     - valid ➡️ 'fastq', 'Per sample raw file', 'Per sample processed file', 'Combined analysis files', 'annData', 'vHDF', 'plink2', 'VCF', 'csv', 'RDS', 'h5', 'Seurat Object', 'bam', 'cram', 'NA'

# replicates seems to simply be taken from the file name...

# 🚨⚠️❗ **Extra field in DATA: source_sample_id**
# 🚨⚠️❗ **Extra field in DATA: replicate count**
# 🚨⚠️❗ **Extra field in DATA: experiment_name**
# 🚨⚠️❗ **Extra field in DATA: experiment_dir**
# 🚨⚠️❗ **Extra field in DATA: file_name_source**
# 🚨⚠️❗ **Extra field in DATA: local_path**
# 🚨⚠️❗ **Extra field in DATA: technology**
# 🚨⚠️❗ **Extra field in DATA: omic**
# 🚨⚠️❗ **Extra field in DATA: time**

# %%
SAMPLE = dfs["SAMPLE"].copy()
# SAMPLE["organism"] = "Human"
# SAMPLE['assay_type'] = "snRNAseq"

# SAMPLE["condition_id"] = SAMPLE["subject_id"].map(condition_mapper)
# Process SAMPLE table
SAMPLE_, SAMPLE_aux, _ = process_table(SAMPLE, "SAMPLE", CDE)

# # ASSAY fields? & PMDBS
# 🚨⚠️❗ **Extra field in SAMPLE: spaceranger_outs_folder**
# 🚨⚠️❗ **Extra field in SAMPLE: brain_region**
# 🚨⚠️❗ **Extra field in SAMPLE: hemisphere**
# 🚨⚠️❗ **Extra field in SAMPLE: region_level_1**
# 🚨⚠️❗ **Extra field in SAMPLE: region_level_2**
# 🚨⚠️❗ **Extra field in SAMPLE: region_level_3**
# 🚨⚠️❗ **Extra field in SAMPLE: RIN**
# 🚨⚠️❗ **Extra field in SAMPLE: molecular_source**
# 🚨⚠️❗ **Extra field in SAMPLE: number_spots_under_tissue**
# 🚨⚠️❗ **Extra field in SAMPLE: assay**
# 🚨⚠️❗ **Extra field in SAMPLE: sequencing_end**
# 🚨⚠️❗ **Extra field in SAMPLE: sequencing_length**
# 🚨⚠️❗ **Extra field in SAMPLE: sequencing_instrument**


# Process DATA table
DATA = dfs["DATA"].copy()
#%%


fastqs = DATA['file_type']=="fastq"

SPATIAL_fields = DATA.loc[~fastqs, ['sample_id', 'source_sample_id', 'file_type', 'experiment_name',
       'experiment_dir', 'file_name_source', 'file_name', 'local_path',
       'file_description', 'file_MD5']]


#%%
SPATIAL = dfs["SPATIAL"].copy()
# ['sample_id', 'geomx_config', 'geomx_dsp_config',
#        'geomx_annotation_file', 'visium_cytassist', 'visium_probe_set',
#        'visium_slide_ref', 'visium_capture_area']


SPATIAL['sample_id'] = SPATIAL_fields['sample_id']
SPATIAL['visium_cytassist'] = SPATIAL_fields['sample_id'] + "/" + SPATIAL_fields['file_name']

SPATIAL_aux = SPATIAL_fields.copy()
SPATIAL_ = SPATIAL.copy()
# %%%
# PMDBS = dfs["PMDBS"]
# PMDBS["hemisphere"] = NULL
PMDBS_, _, _ = process_table(SAMPLE.copy(), "PMDBS", CDE)



ASSAY_RNAseq_, _, _ = process_table(SAMPLE.copy(), "ASSAY_RNAseq", CDE)

# %%
STUDY = dfs["STUDY"].copy()
STUDY.loc[0,"metadata_version_date"] = METADATA_VERSION_DATE
STUDY["ASAP_team_name"] = "TEAM-SCHERZER"
STUDY["metadata_tables"] = f"{tables}"
STUDY_, STUDY_aux, _ = process_table(STUDY, "STUDY", CDE)

# %%
PROTOCOL = dfs["PROTOCOL"].copy()
PROTOCOL_, PROTOCOL_aux, _ = process_table(PROTOCOL, "PROTOCOL", CDE)


# %%


# %% [markdown]
# CONDITION
CONDITION = dfs["CONDITION"].copy()
CONDITION["condition_id"] = SUBJECT_["gp2_phenotype"].unique()
CONDITION["intervention_name"] = "Case-Control"
CONDITION["intervention_id"] = CONDITION["condition_id"].map({"PD": "Case", "Control": "Control", "Prodromal": "Case"})
CONDITION = CONDITION.fillna(NULL)
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
SPATIAL_.to_csv(metadata_path / "SPATIAL.csv", index=False)

SAMPLE_aux.to_csv(metadata_path / "SAMPLE_aux.csv", index=False)
CLINPATH_aux.to_csv(metadata_path / "CLINPATH_aux.csv", index=False)
DATA_aux.to_csv(metadata_path / "DATA_aux.csv", index=False)
SUBJECT_aux.to_csv(metadata_path / "SUBJECT_aux.csv", index=False)
SPATIAL_aux.to_csv(metadata_path / "SPATIAL_aux.csv", index=False)

# %%\# %%





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
