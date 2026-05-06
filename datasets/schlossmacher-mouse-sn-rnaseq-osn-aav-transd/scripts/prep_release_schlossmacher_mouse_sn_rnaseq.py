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
# intake_doc = (
#     ds_path / "refs/Dataset Information Template_JJ.docx"
# )
dataset_version = "1.0"

# %%
map_path = root_path / "asap-ids/master"
suffix = "ids"
# %%
# %%
# %%
# final export to v3.2 (current metadat)
#########################################################

dfs = load_tables(metadata_path, tables)
create_metadata_package(dfs, metadata_path, schema_version)

# %%
# %%
#########################################################
#
#  prepare for release.  ASAP_ids. bump version, etc...
#
#########################################################

prep_release_metadata(ds_path,schema_version,map_path, suffix, spatial=spatial, source=source)


# %%
# bucket_files = pd.read_csv("../refs/manifest.txt",sep="\t", names=["uri"])

from crn_utils.release_util import get_stats_table

# dfs = get_crn_release_metadata(ds_path,schema_version,map_path, suffix, spatial=spatial, source=source)

dfs = load_tables(metadata_path/"release", tables)

report,df = get_stats_table(dfs, source)


print(df)
report
# %%
