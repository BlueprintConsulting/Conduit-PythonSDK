# Introduction

This is a Python SDK to abstract the REST endpoints for the Blueprint Conduit product.

# Installation

NOTE: These steps aren't for Windows, but works for POSIX systems. Use whatever Windows uses in place of these instructions.

* Python3 is a prerequisite

* Start in a new directory, go to it:
```
mkdir conduit-sdk && cd conduit-sdk
```
* Install a virtualenv.
```
python3 -m venv .venv
```
* Go into the virtualenv.
```
. .venv/bin/activate
```
* Install bpcs-conduit
```
pip install bpcs-conduit
```
* Save this to a file called `test.py`:
```
from conduit_pkg.client import getDatabases
dbs = getDatabases()
for db in dbs:
    print(db)
```
* Execute with the CONDUIT_TOKEN, CONDUIT_SERVER
```
CONDUIT_SERVER=<server name> CONDUIT_TOKEN=<your token> python test.py
```
You should get something similar as output:
```
Database: file_blob
Database: redshift_redshift
Database: es_elasticsearch
Database: awss3_s3
```