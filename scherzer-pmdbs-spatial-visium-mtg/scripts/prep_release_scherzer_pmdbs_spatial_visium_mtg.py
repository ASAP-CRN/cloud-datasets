# %%
# script for generating the source-of-truth metadata for a release and updating the
#
# %%

# STEPS
# 1. metadata QC  scripts/QC_scherzer_spatial_visium_mtg.py
#    a. QC
#    b. extract DOI info

# 2. prep release scripts/prep_scherzer_spatial_visium_mtg.py 
#    a. extract DOI info
#    b. create DOI_readme.txt
#    c. mint DOI 

# 3. prep release scripts/prep_release_scherzer_spatial_visium_mtg.py 
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
from crn_utils.doi import *
from crn_utils.release_util import create_metadata_package, prep_release_metadata

%load_ext autoreload
%autoreload 2
##############

# %%
root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata")
datasets_path = root_path / "datasets"

team = "scherzer"
dataset_name = "spatial-visium-mtg"
source = "pmdbs"
long_dataset_name = f"{team}-{source}-{dataset_name}"
spatial = True
# %%
tables = PMDBS_TABLES + ["SPATIAL"]
schema_version = "v3.3"

# %%
ds_path = datasets_path / long_dataset_name

# %%
root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata")
datasets_path = root_path / "datasets"
ds_path = datasets_path / long_dataset_name
metadata_path = ds_path / "metadata"
dataset_version = "1.0"

# %%
map_path = root_path / "asap-ids/master"
suffix = "ids"

# %%
#########################################################

dfs = load_tables(metadata_path, tables)
create_metadata_package(dfs, metadata_path, schema_version)


# %%
#########################################################
#
#  prepare for release.  ASAP_ids. bump version, etc...
#
#########################################################

prep_release_metadata(ds_path,schema_version,map_path, suffix, spatial=spatial, source=source)


# %%