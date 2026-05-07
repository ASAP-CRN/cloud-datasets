# %% 
# ASAP CRN Metadata creation
#
# Cohort Mouse sn RNAseq. 
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

from crn_utils.constants import *
from crn_utils.doi import *

%load_ext autoreload
%autoreload 2



# %%
root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata")
datasets_path = root_path / "datasets"

# %%
# ### Starting with v3.3 table
team = "cohort"
dataset_name = "sc-rnaseq"
source = "mouse"
intake_schema_version = "v3.3"
spatial = False

metadata_version = intake_schema_version
schema_version = intake_schema_version



# %%\# %%
CDEv3 = read_CDE(intake_schema_version)

CDE = CDEv3

tables = MOUSE_TABLES

# force order here to make sure we generate consistent IDs... should actually be okay, but just in case
datasets = [
    'cragg-mouse-sn-rnaseq-striatum',
    'biederer-mouse-sc-rnaseq',
]


release = "v4.0.0"
# %%


combined_dfs = {}
combined_raw_dfs = pd.DataFrame()
for dataset in datasets:

    print(f'Processing {dataset}')

    ds_path = datasets_path / dataset 
    # hack.  use the base release path...
    ds_release_path = ds_path / "metadata/release" 
    # ds_release_path = ds_path / "metadata/release" / release

    if not ds_release_path.exists():
        print(f'    {dataset} does not have a {release} release. .')
        # make a copy for the release
        ds_release_path.mkdir(parents=True, exist_ok=True)
        dfs = load_tables(ds_path / "metadata/release" , tables)
        export_meta_tables(dfs, ds_release_path)
        # export_meta_tables(dfs, metadata_path)
        write_version(schema_version, ds_release_path / "cde_version")
        print(f' wrote a {release} archive .')

    else:
        dfs = load_tables(ds_release_path, tables)


    raw_df = read_meta_table(ds_path / "file_metadata/raw_files.csv")



    if combined_dfs == {}: # first time through
        combined_dfs = dfs
        combined_raw_dfs = raw_df
    else:
        for tab in tables:
            if tab not in dfs:
                continue
            combined_dfs[tab] = pd.concat([combined_dfs[tab], dfs[tab]], ignore_index=True)
        
        combined_raw_dfs = pd.concat([combined_raw_dfs, raw_df], ignore_index=True)




# %% [markdown]
dataset_path = datasets_path / f"{team}-{source}-{dataset_name}"

dataset_path.mkdir(parents=True, exist_ok=True)
    
final_metadata_path = dataset_path / "metadata/release"

# %%

if not (final_metadata_path / release).exists():
    (final_metadata_path / release).mkdir(parents=True, exist_ok=True)

export_meta_tables(combined_dfs, final_metadata_path)

export_meta_tables(combined_dfs, final_metadata_path / release)
# export_meta_tables(dfs, metadata_path)
write_version(schema_version, final_metadata_path / "cde_version")
write_version(schema_version, final_metadata_path / release/ "cde_version")

# %%
file_metadata_path = dataset_path / "file_metadata"
if not file_metadata_path.exists():
    file_metadata_path.mkdir(parents=True, exist_ok=True)
file_metadata_path_release = file_metadata_path / release
if not file_metadata_path_release.exists():
    file_metadata_path_release.mkdir(parents=True, exist_ok=True)

combined_raw_dfs.to_csv(file_metadata_path / "raw_files.csv", index=False)
combined_raw_dfs.to_csv(file_metadata_path_release / "raw_files.csv", index=False)

# %%
# %%
# write collection version 
collection_version = "v1.0.0"
write_version(collection_version, final_metadata_path / "collection_version")
write_version(collection_version, final_metadata_path / release/ "collection_version")
# %%
write_version(collection_version, file_metadata_path / "collection_version")
write_version(collection_version, file_metadata_path_release / "collection_version")


# %%

write_version(collection_version, dataset_path / "version")

# %%
