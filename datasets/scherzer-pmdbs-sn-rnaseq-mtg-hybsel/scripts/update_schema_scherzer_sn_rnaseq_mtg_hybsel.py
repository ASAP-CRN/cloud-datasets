# %%
# script for updating the schema for a dataset .
# i.e. v3.1 to v3.2 for the Release v3.0.0
#

import pandas as pd
import json
from pathlib import Path
from crn_utils.util import (
    NULL,
    export_meta_tables,
    load_tables,
    write_version,
    read_CDE,
)

from crn_utils.asap_ids import *
from crn_utils.constants import *
from crn_utils.doi import *
from crn_utils.release_util import create_metadata_package, prep_release_metadata
from crn_utils.update_schema import *


%load_ext autoreload
%autoreload 2
##############
# %%
root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata")
datasets_path = root_path / "datasets"

team = "scherzer"
dataset_name = "sn-rnaseq-mtg-hybsel"
source = "pmdbs"
long_dataset_name = f"{team}-{source}-{dataset_name}"
spatial = False

# %%

tables = MOUSE_TABLES if source == "mouse" else PMDBS_TABLES
tables = tables + ["SPATIAL"] if spatial else tables
schema_version = "v3.0"

# %%
ds_path = datasets_path / long_dataset_name

# %%
root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata")
datasets_path = root_path / "datasets"
ds_path = datasets_path / long_dataset_name
metadata_path = ds_path / "metadata"

dataset_version = "1.0"

# load jsons
doi_path = ds_path / "DOI"
with open(doi_path / f"project.json", "r") as f:
    dataset_info = json.load(f)

dataset_info['dataset_name'] = dataset_info.get('dataset_title',long_dataset_name)

# %%
#  v3.0 (current release metadata)
#########################################################

input_schema = 'v3.0'
schema_version = 'v3.2'
CDEv3_2 = read_CDE(schema_version)

input_metadata = metadata_path / input_schema

dfs3_0 = load_tables(input_metadata, tables)

# %%
dfs3_2 = update_tables_v3_0_to_3_2(dfs3_0, dataset_info, CDEv3_2)


# %%
#########################################################
#
#  create metadata packages
#
#########################################################

export_meta_tables(dfs3_2, metadata_path)

create_metadata_package(dfs3_2, metadata_path, schema_version)

# %%
map_path = root_path / "asap-ids/master"
suffix = "ids"
prep_release_metadata(ds_path,schema_version,map_path, suffix, spatial=spatial, source=source)

# %%
