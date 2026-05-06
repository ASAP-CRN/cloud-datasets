#
# script for generating the source-of-truth metadata for a release and updating the
#
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
from crn_utils.file_metadata import (
                gen_raw_bucket_summary_flat,
                make_file_metadata, 
                update_data_table_with_gcp_uri,
                update_spatial_table_with_gcp_uri,
)
from crn_utils.bucket_util import authenticate_with_service_account

%load_ext autoreload
%autoreload 2
##############
# %%
root_path = Path.home() / ("Projects/ASAP/asap-crn-cloud-dataset-metadata")
datasets_path = root_path / "datasets"


pmdbs_tables = PMDBS_TABLES
spatial_tables = pmdbs_tables + ["SPATIAL"]

##############
# %%
map_path = root_path / "asap-ids/master"
suffix = "ids"

retval = load_pmdbs_id_mappers(map_path, suffix)
datasetid_mapper, subjectid_mapper, sampleid_mapper, gp2id_mapper, sourceid_mapper = (
    retval
)

# %%
schema_version = "v3.1"
CDE = read_CDE(schema_version)
asap_ids_df = read_CDE_asap_ids()
# asap_ids_df = read_CDE_asap_ids()
asap_ids_schema = asap_ids_df[["Table", "Field"]]


# %%
dataset_names = [
    "edwards-pmdbs-spatial-geomx-th",
    "vila-pmdbs-spatial-geomx-thlc",
    "vila-pmdbs-spatial-geomx-unmasked",
]
datasets_short = [
    "edwards-pmdbs-spatial-geomx-th",
    "vila-pmdbs-spatial-geomx-unmasked",
    "vila-pmdbs-spatial-geomx-thlc",
]
# %%

for ds in datasets_short:
    ds_path = datasets_path / ds
    print(f"Processing {ds}")
    # ds_path.mkdir(parents=True, exist_ok=True)
    mdata_path = ds_path / "metadata"
    tables = [
        table
        for table in mdata_path.iterdir()
        if table.is_file() and table.suffix == ".csv"
    ]

    table_names = [table.stem for table in tables if table.stem in spatial_tables]

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

    if spatial:
        get_image_bucket_summary(raw_bucket_name, file_metadata_path, dataset_name)

    make_file_metadata(ds_path, file_metadata_path, dfs["DATA"])

    # export the tables to the metadata directory in a release subdirectory
    out_dir = ds_path / "metadata/release"
    out_dir.mkdir(parents=True, exist_ok=True)

    dfs["STUDY"] = update_study_table_with_doi(dfs["STUDY"], ds_path)
    dfs["DATA"] = update_data_table_with_gcp_uri(dfs["DATA"], ds_path)
    dfs["SPATIAL"] = update_spatial_table_with_gcp_uri(dfs["SPATIAL"], ds_path, visium=False)

    export_meta_tables(dfs, out_dir)
    write_version(schema_version, out_dir / "cde_version")




# %%
export_map_path = root_path / "asap-ids/master"
export_pmdbs_id_mappers(
    export_map_path,
    suffix,
    datasetid_mapper,
    subjectid_mapper,
    sampleid_mapper,
    gp2id_mapper,
    sourceid_mapper,
)










####################
# %%
agg_DATA = pd.DataFrame()
agg_SPATIAL = pd.DataFrame()
agg_SAMPLE = pd.DataFrame()
agg_CONDITION = pd.DataFrame()

for ds in datasets_short:
    ds_path = datasets_path / ds
    print(f"Processing {ds}")
    # ds_path.mkdir(parents=True, exist_ok=True)
    mdata_path = ds_path / "metadata" / "release"

    # table_names = [table.stem for table in tables if table.stem in spatial_tables]

    dfs = load_tables(mdata_path, spatial_tables)

    agg_DATA = pd.concat([agg_DATA, dfs["DATA"]])
    agg_SPATIAL = pd.concat([agg_SPATIAL, dfs["SPATIAL"]])
    agg_SAMPLE = pd.concat([agg_SAMPLE, dfs["SAMPLE"]])
    agg_CONDITION = pd.concat([agg_CONDITION, dfs["CONDITION"]])

#################
# %%

agg_DATA['sample_name'] = agg_DATA['sample_id'] + "_" + agg_DATA['replicate']


# %%
from crn_utils.asap_ids import *


# %%
clinpath_df = dfs["CLINPATH"]
sample_df = dfs["SAMPLE"]

# %%


# # add ASAP_subject_id to the SUBJECT tables
# output = _generate_human_subject_ids(subjectid_mapper,
#                                         gp2id_mapper,
#                                         sourceid_mapper,
#                                         subjec_ids_df)
# subjectid_mapper, gp2id_mapper,sourceid_mapper = output
# %%
# # %%
# # isolate generation of the IDs and adding to mappers from updating the tables.
# def _update_pmdbs_id_mappers(clinpath_df,
#                         sample_df,
#                         long_dataset_name,
#                         datasetid_mapper,
#                         subjectid_mapper,
#                         sampleid_mapper,
#                         gp2id_mapper,
#                         sourceid_mapper):
#     """
#     read in the CLINPATH and SAMPLE data tables, generate new ids, update the id_mappers

#     return updated id_mappers
#     """

#     _, datasetid_mapper = _generate_asap_dataset_id(datasetid_mapper, long_dataset_name)

subject_ids_df = clinpath_df[["subject_id", "source_subject_id", "GP2_id"]]

# # add ASAP_subject_id to the SUBJECT tables
# output = _generate_human_subject_ids(subjectid_mapper,
#                                         gp2id_mapper,
#                                         sourceid_mapper,
#                                         subject_ids_df)

# return datasetid_mapper, subjectid_mapper, sampleid_mapper, gp2id_mapper, sourceid_mapper


# extract the max value of the mapper's third (last) section ([2] or [-1]) to get our n
if bool(subjectid_mapper):
    n = max([int(v.split("_")[2]) for v in subjectid_mapper.values() if v]) + 1
else:
    n = 1
nstart = n

# %%

# # ids_df = subject_df[['subject_id','source_subject_id', 'AMPPD_id', 'GP2_id']].copy()
# ids_df = subject_ids_df.copy()

# # might want to use 'source_subject_id' instead of 'subject_id' since we want to find matches across teams
# # shouldn't actually matter but logically cleaner
# uniq_subj = ids_df['subject_id'].unique()
# dupids_mapper = dict(zip(uniq_subj,
#                     [num + nstart for num in range(len(uniq_subj))] ))

# n_asap_id_add = 0
# n_gp2_id_add = 0
# n_source_id_add = 0

# df_dup_chunks = []
# id_source = []
# for subj_id, samp_n in dupids_mapper.items():
#     df_dups_subset = ids_df[ids_df.subject_id==subj_id].copy()

#     # check if gp2_id is known
#     # NOTE:  the gp2_id _might_ not be the GP2ID, but instead the GP2sampleID
#     #        we might want to check for a trailing _s\d+ and remove it
#     #        need to check w/ GP2 team about this.  The RepNo might be sample timepoint...
#     #        and hence be a "subject" in our context
#     #    # df['GP2ID'] = df['GP2sampleID'].apply(lambda x: ("_").join(x.split("_")[:-1]))
#     #    # df['SampleRepNo'] = df['GP2sampleID'].apply(lambda x: x.split("_")[-1])#.replace("s",""))

#     gp2_id = None
#     add_gp2_id = False
#     # force skipping of null GP2_ids
#     if df_dups_subset['GP2_id'].nunique() > 1:
#         print(f"subj_id: {subj_id} has multiple gp2_ids: {df_dups_subset['GP2_id'].to_list()}... something is wrong")
#         #TODO: log this
#     elif not df_dups_subset['GP2_id'].dropna().empty: # we have a valide GP2_id
#         gp2_id = df_dups_subset['GP2_id'].values[0] # values because index was not reset

#     if gp2_id in set(gp2id_mapper.keys()):
#         asap_subj_id_gp2 = gp2id_mapper[gp2_id]
#     else:
#         add_gp2_id = True
#         asap_subj_id_gp2 = None

#     # check if source_id is known
#     source_id = None
#     add_source_id = False
#     if df_dups_subset['source_subject_id'].nunique() > 1:
#         print(f"subj_id: {subj_id} has multiple source ids: {df_dups_subset['source_subject_id'].to_list()}... something is wrong")
#         #TODO: log this
#     elif df_dups_subset['source_subject_id'].isnull().any():
#         print(f"subj_id: {subj_id} has no source_id... something is wrong")
#         #TODO: log this
#     else: # we have a valide source_id
#         #TODO: check for `source_subject_id` naming collisions with other teams
#         #      e.g. check the `biobank_name`
#         source_id = df_dups_subset['source_subject_id'].values[0]

#     if source_id in set(sourceid_mapper.keys()):
#         asap_subj_id_source = sourceid_mapper[source_id]
#     else:
#         add_source_id = True
#         asap_subj_id_source = None

#     # TODO: add AMPPD_id test/mapper

#     # check if subj_id is known
#     add_subj_id = False
#     # check if subj_id (subject_id) is known
#     if subj_id in set(subjectid_mapper.keys()): # duplicate!!
#         # TODO: log this
#         # TODO: check for `subject_id` naming collisions with other teams
#         asap_subj_id = subjectid_mapper[subj_id]
#     else:
#         add_subj_id = True
#         asap_subj_id = None

#     # TODO:  improve the logic here so gp2 is the default if it exists.?
#     #        we need to check the team_id to make sure it's not a naming collision on subject_id
#     #        we need to check the biobank_name to make sure it's not a naming collision on source_subject_id

#     testset = set((asap_subj_id, asap_subj_id_gp2, asap_subj_id_source))
#     if None in testset:
#         testset.remove(None)

#     # check that asap_subj_id is not disparate between the maps
#     if len(testset) > 1:
#         print(f"collission between our ids: {(asap_subj_id, asap_subj_id_gp2, asap_subj_id_source)=}")
#         print(f"this is BAAAAD. could be a naming collision with another team on `subject_id` ")

#     if len(testset) == 0:  # generate a new asap_subj_id
#         # print(samp_n)
#         asap_subject_id = f"{source.upper()}_{samp_n:06}"
#         # df_dups_subset.insert(0, 'ASAP_subject_id', asap_subject_id, inplace=True)
#     else: # testset should have the asap_subj_id
#         asap_subject_id = testset.pop() # but where did it come from?
#         # print(f"found {subj_id }:{asap_subject_id} in the maps")

#     src = []
#     if add_subj_id:
#         # TODO:  instead of just adding we should check if it exists...
#         subjectid_mapper[subj_id] = asap_subject_id
#         n_asap_id_add += 1
#         src.append('asap')

#     if add_gp2_id and gp2_id is not None:
#         # TODO:  instead of just adding we should check if it exists...
#         gp2id_mapper[gp2_id] = asap_subject_id
#         n_gp2_id_add += 1
#         src.append('gp2')

#     if add_source_id and source_id is not None:
#         # TODO:  instead of just adding we should check if it exists...
#         sourceid_mapper[source_id] = asap_subject_id
#         n_source_id_add += 1
#         src.append('source')


#     df_dup_chunks.append(df_dups_subset)
#     id_source.append(src)


# df_dups_wids = pd.concat(df_dup_chunks)
# assert df_dups_wids.sort_index().equals(subject_df)
# print(f"added {n_asap_id_add} new asap_subject_ids")
# print(f"added {n_gp2_id_add} new gp2_ids")
# print(f"added {n_source_id_add} new source_ids")

# # print(id_source)

# return subjectid_mapper, gp2id_mapper, sourceid_mapper

# %%

subjectid_mapper, gp2id_mapper, sourceid_mapper = output

sample_ids_df = sample_df[["sample_id", "subject_id", "source_sample_id"]]
sampleid_mapper = generate_human_sample_ids(
    subjectid_mapper, sampleid_mapper, sample_ids_df
)


sample_ids_df = sample_df[["sample_id", "subject_id", "source_sample_id"]]
sampleid_mapper = generate_human_sample_ids(
    subjectid_mapper, sampleid_mapper, sample_ids_df
)


# update_pmdbs_id_mappers
# %%

# %%
ASAP_sample_id_tables = asap_ids_schema[asap_ids_schema["Field"] == "ASAP_sample_id"][
    "Table"
].to_list()
ASAP_subject_id_tables = asap_ids_schema[asap_ids_schema["Field"] == "ASAP_subject_id"][
    "Table"
].to_list()

BUCKETS = pd.DataFrame(
    columns=[
        "ASAP_team_id",
        "ASAP_dataset_id",
        "dataset_name",
        "bucket_name",
        "source",
        "team",
    ]
)

combined_dfs = {}
for dataset in datasets:
    dataset_name = dataset.name
    print(f"Processing {dataset_name}")
    tables = [
        table
        for table in (dataset / release_dir).iterdir()
        if table.is_file() and table.suffix == ".csv"
    ]

    table_names = [table.stem for table in tables if table.stem in pmdbs_tables]

    dfs = load_tables(dataset / release_dir, table_names)

    release_dir = "metadata/release"

    if combined_dfs == {}:  # first time through
        combined_dfs = dfs
    else:
        for tab in pmdbs_tables:
            if tab not in dfs:
                continue
            combined_dfs[tab] = pd.concat(
                [combined_dfs[tab], dfs[tab]], ignore_index=True
            )

    bucket_df = dfs["STUDY"][["ASAP_team_id", "ASAP_dataset_id"]]
    bucket_df["dataset_name"] = dataset.name
    bucket_df["bucket_name"] = f"gs://asap-raw-team-{dataset.name}"
    bucket_df["source"] = "-".join(dataset.name.split("-")[2:])
    bucket_df["team"] = dataset.name.split("-")[0]

    BUCKETS = pd.concat([BUCKETS, bucket_df], ignore_index=True)


# %%
for table, df in combined_dfs.items():
    df.to_csv(metadata_path / f"{table}.csv", index=False)
