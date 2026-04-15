# %%
# script for generating the source-of-truth metadata for a release and updating the
#
# %%

# STEPS
# 1. metadata QC  scripts/QC_biederer_mouse_sc_rnaseq.py
#    a. QC

# 2. prep release scripts/prep_biederer_mouse_sn.py (this script)
#    a. extract DOI info
#    b. create DOI_readme.txt
#    c. mint DOI 

# 3. prep release scripts/prep_release_schlossmacher_mouse_sn_rnaseq.py 
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
team = "schlossmacher"
dataset_name = "sn-rnaseq-osn-aav-transd"
source = "mouse"
long_dataset_name = f"{team}-{source}-{dataset_name}"
spatial = False

# %%

tables = MOUSE_TABLES if source == "mouse" else PMDBS_TABLES
tables = tables + ["SPATIAL"] if spatial else tables
schema_version = "v3.3"

# %%
ds_path = datasets_path / long_dataset_name

# %%
root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata")
datasets_path = root_path / "datasets"
ds_path = datasets_path / long_dataset_name
metadata_path = ds_path / "metadata"
intake_doc = (
    ds_path / "refs/Dataset Information Template_JJ.docx"
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

setup_DOI_info(ds_path, intake_doc, publication_date="2025-10-03")
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

    # not sure why this is failing...
    deposition, metadata = create_draft_doi(zenodo, ds_path, version=current_dataset_version)
    v1_beta_doi_id =  "17358328"  #deposition['id']


    # dataset_doi = "17358327"

    zenodo.set_deposition_id(v1_beta_doi_id)

    # zenodo.set_deposition_id("17358327")

    grants = local_metadata.pop("grants")
    # fix by hand

    file_path = ds_path / "DOI" / f"{ds_path.name}_README.pdf"
    deposition = update_doi_metadata(zenodo, v1_beta_doi_id, local_metadata)

    
    # why does this URL FAIL??  BAD REQUEST 400.... something about the metadata  is breaking this
    #. creating a new DOI also fails....
    # import requests
    # import json
    # data_payload = {"metadata": local_metadata}
    # url = "https://zenodo.org/api/deposit/depositions/17358328"
    # access_token="GPdQkp9nbPf3TFy6xswMZOJcwit8dZ6T8yQElTKGNAO3XwWFJeECaUsNNXTD"
    # r = requests.put(
    #     # f"{self._endpoint}/deposit/depositions/{self.deposition_id}",
    #     url,
    #     params={"access_token": access_token},
    #     data=json.dumps(data_payload),
    #     headers={"Content-Type": "application/json"},
    # )

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
    doi_id = "17358328"
    zenodo.deposition_id = doi_id

    deposition = zenodo.deposition

    deposition = bump_doi_version(zenodo, doi_id)

    metadata = deposition.get("metadata")
    new_doi_id = f"{deposition['id']}"
    # deposition, metadata = create_draft_doi(zenodo, ds_path)
    zenodo.deposition_id = new_doi_id # 17402344
    local_metadata = create_draft_metadata(ds_path, version="1.0")
    metadata = local_metadata
    metadata['version'] = '1.0'

    metadata.pop("grants") # no idea why grants makes this fails...
    deposition = update_doi_metadata(zenodo, new_doi_id, metadata)
    archive_deposition_local(ds_path, "pre-release-deposition", deposition)


# %%
#########################################################
#
#  prepare for release.  ASAP_ids. bump version, etc...
#
#########################################################

prep_release_metadata(ds_path,schema_version,map_path, suffix, spatial=spatial, source=source)

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
    new_doi_id = ''
    # deposition = zenodo.all_depositions[doi_id] #get_published_deposition(zenodo,doi_id)
    deposition = publish_doi(zenodo, new_doi_id)
    archive_deposition_local(ds_path, "final-deposition", deposition)

    finalize_DOI(ds_path, deposition)


