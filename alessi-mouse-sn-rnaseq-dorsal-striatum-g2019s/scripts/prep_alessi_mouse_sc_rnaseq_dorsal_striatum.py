# %%
# script for generating the source-of-truth metadata for a release and updating the
#
# %%

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
from crn_utils.util import (
    NULL,
    export_meta_tables,
    load_tables,
    write_version,
)

from crn_utils.asap_ids import *
from crn_utils.constants import *
from crn_utils.file_metadata import gen_raw_bucket_summary, update_data_table_with_gcp_uri
from crn_utils.doi import *
from crn_utils.zenodo_util import ZenodoClient
from crn_utils.release_util import create_metadata_package, prep_release_metadata

%load_ext autoreload
%autoreload 2
##############

# %%
root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata")
datasets_path = root_path / "datasets"


# %%
team = "alessi"
dataset_name = "sn-rnaseq-dorsal-striatum-g2019s"
source = "mouse"
long_dataset_name = f"{team}-{source}-{dataset_name}"
spatial = False

# %%

tables = MOUSE_TABLES if source == "mouse" else PMDBS_TABLES
tables = tables + ["SPATIAL"] if spatial else tables
schema_version = "v3.1"

# %%
ds_path = datasets_path / long_dataset_name

# %%
root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata")
datasets_path = root_path / "datasets"
ds_path = datasets_path / long_dataset_name
metadata_path = ds_path / "metadata"
intake_doc = (
    ds_path / "refs/Dataset Information_Yu-En_Pfeffer_lab_Team_Alessi.docx"
)
dataset_version = "1.0"

# %%
map_path = root_path / "asap-ids/master"
suffix = "ids"
# %%
# %%
#########################################################
#
#  create metadata package
#
#########################################################
# INGEST DOI DOCS

setup_DOI_info(ds_path, intake_doc, publication_date="2025-09-18")
# %%
# final export to v3.1 (current metadat)
#########################################################

dfs = load_tables(metadata_path, tables)
create_metadata_package(dfs, metadata_path, schema_version)



# %%
#########################################################
#
#  setup DOI. (or bump)
#
#########################################################

# %%
if False:
    zenodo = setup_zenodo()

    current_dataset_version = "0.1"
    local_metadata = create_draft_metadata(ds_path, version=current_dataset_version)

    deposition, metadata = create_draft_doi(zenodo, ds_path, version=current_dataset_version)
    v1_beta_doi_id = deposition['id']

    # extra draft id: 17401829

    file_path = ds_path / "DOI" / f"{ds_path.name}_README.pdf"
    # deposition = update_doi_metadata(zenodo, v1_beta_doi_id, metadata)
    deposition = add_anchor_file_to_doi(zenodo,  file_path, v1_beta_doi_id)

    deposition = publish_doi(zenodo, v1_beta_doi_id)

    archive_deposition_local(ds_path, "beta-deposition", deposition)
    finalize_DOI(ds_path, deposition)

# # %%
#########################################################
#
#  create acceptance v1.0 DOI. (or bump)
#
#########################################################
# %%
if False:
    zenodo = setup_zenodo()
    # doi_id = get_doi_from_dataset(ds_path, version=True) #17149267
    doi_id = "17212216"
    zenodo.deposition_id = doi_id

    deposition = zenodo.deposition

    deposition = bump_doi_version(zenodo, doi_id)

    metadata = deposition.get("metadata")
    new_doi_id = f"{deposition['id']}"
    # deposition, metadata = create_draft_doi(zenodo, ds_path)
    zenodo.deposition_id = new_doi_id
    local_metadata = create_draft_metadata(ds_path, version="1.0")
    metadata = local_metadata
    metadata['version'] = '1.0'

    deposition = update_doi_metadata(zenodo, new_doi_id, metadata)
    archive_deposition_local(ds_path, "pre-release-deposition", deposition)


########################################################
########################################################
########################################################
########################################################
########################################################
########################################################
# %%
#########################################################
#
#  prepare for release.  ASAP_ids. bump version, etc...
#
#########################################################

prep_sc_release_metadata(ds_path,schema_version,map_path, suffix, spatial=spatial, source=source)

# %%
#########################################################
#
# RELEASE publish 
#
#########################################################

# TODO: check if below is what we want
#  
# 
if False:
    zenodo = setup_zenodo()
    doi_id = get_doi_from_dataset(ds_path, version=True)
    new_doi_id = '16929452'
    # deposition = zenodo.all_depositions[doi_id] #get_published_deposition(zenodo,doi_id)
    deposition = publish_doi(zenodo, new_doi_id)
    archive_deposition_local(ds_path, "final-deposition", deposition)

    finalize_DOI(ds_path, deposition)
