# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # Tag images from a file system
# This notebook demonstrates how to use Splash-ML to scan a file system for files and then introduce them to splash-ml, how to save tag sets using the TagService in the tagging packages, and how to query on those tags.
# 
# The notebook uses mongomock to mimic a mongo database instance in memory. This could easily be replaced with a MongoClient from pymongo.

# %%
import sys; sys.path.insert(0, '../..')
from datetime import datetime
import glob
import os
from pathlib import Path

import pandas as pd

from pymongo import MongoClient

from tagging.model import FileAsset, Tagger, TaggingEvent, AssetType, Tag, LABEL_NAME
from tagging.tag_service import TagService
from tagging.util.files import anonymize_copy

# %% [markdown]
# First, let's do some setup. We'll create a monomock instance which will be used by as a location to ingest data into and as a place to save and search on tags.
# %% [markdown]
# Let's create a properties_callback. This callable is used by the `ETLExecutor` to build up a list of detected properties as it 

# %%
def properties_callback(path):
    metadata = {}
    if 'agb' in path:
        metadata['scan_type'] = 'agb_calibration'
    return metadata


src_root_path = os.path.join(Path(__file__).parent, "labelled")
src_relative_path = "labelled"
dest_root = os.path.join(Path(__file__).parent, "anonymous", src_relative_path)

db = MongoClient()
path = os.path.join(src_root_path, '**/*.tif*')

#use glob to find all the files to ingest
paths = glob.glob(path, recursive=True)
tag_svc = TagService(db, db_name='tagging')
tagger = tag_svc.create_tagger(Tagger(type="human", name="build_tag notebook"))
tagging_event = tag_svc.create_tagging_event(TaggingEvent(tagger_id=tagger.uid, run_time=datetime.now()))

labels = pd.read_csv(os.path.join(src_root_path, "labels.csv"), header=[0])

for src_root_path in paths:
    print("\nreading and transforming file: " + src_root_path)
    anonymize_copy(src_root_path, src_relative_path, dest_root)
    asset = FileAsset(uri=src_root_path)
    # get label row from csv using file name
    file_name = os.path.splitext(os.path.split(src_root_path)[1])[0]
    row = labels.loc[labels['image name'] == int(file_name)]
    tags = []
    tags.append(Tag(name=LABEL_NAME, value="peaks", confidence=row['peaks'].values[0], event_id=tagging_event.uid))
    tags.append(Tag(name=LABEL_NAME, value="rings", confidence=row['rings'].values[0], event_id=tagging_event.uid))
    tags.append(Tag(name=LABEL_NAME, value="rods", confidence=row['rods'].values[0], event_id=tagging_event.uid))
    tags.append(Tag(name=LABEL_NAME, value="arcs", confidence=row['arcs'].values[0], event_id=tagging_event.uid))
    asset.tags = tags
    tag_svc.create_asset(asset)


# %% [markdown]
# Now that we have loaded the tagging database, we can do some queries on what we have. First, find random tagging events.

assets_with_peaks = tag_svc.find_assets(**{"tags.value": "peaks"})
for asset in assets_with_peaks:
    tag = [tag for tag in asset.tags if tag.value == "peaks"]
    print(f"{asset.uri} has peak tag: {tag}")
