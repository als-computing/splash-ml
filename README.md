
# Splash-ML
Splash-ML is a project intended to provide support for training and running classification ML models using a database to store information about assets and tagging events.

## Background
The Splash-ML project takes inspiration from the Splash project in that it uses metadata and data captured from scientific instruments and provides a service to add usefulness for the data. Like Splash, Splash-ML uses, in part, the Data Broker as a source for raw data. This data can be ingested directly from Bluesky-enabled beamlines, or ingested after the fact with custom ingestion code. Once ingested into Data Broker, Splash-ML provides a Tagging service with functions for storing and querying tag sets for tagging events that can come from a variety of sources (e.g. human taggers, tagging from trained machine learning models). 

With a query-able database of tags and related assets, Splash-ML supports a variety of use cases with a central theme of making it easier to access collected data, metadata and tags in support of training and running machine learning models. See [Use Cases](docs/use_cases.md) for details.

Splash-ML currently consists of two separate projects in one repository (which might be separated in the future). 

### ETL
The etl package contains an Extract, Transform and Load facility for:
* taking file system images
* anonymizing the images
* converting them to ML-consumable formats (jpg and tiff)
* ingesting into databroker with metadata extracted from names and paths

### TagService
The Tag Service provides the facility for storing and query tags sets, storing information about "tagging events" and linking them to assets stored in databroker. See [Tag Service Model](docs/tag_svc_model.md) for information on the data model. In the future, we'll add information about the tag service API. 

# Copyright
Splash-ML Copyright (c) 2020, The Regents of the University of California, 
through Lawrence Berkeley National Laboratory (subject to receipt of 
any required approvals from the U.S. Dept. of Energy).  All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Intellectual Property Office at
IPO@lbl.gov.

NOTICE.  This Software was developed under funding from the U.S. Department
of Energy and the U.S. Government consequently retains certain rights.  As
such, the U.S. Government has been granted for itself and others acting on
its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the
Software to reproduce, distribute copies to the public, prepare derivative 
works, and perform publicly and display publicly, and to permit others to do so.

