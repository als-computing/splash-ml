# Setup
This notebook can be run locally. Here we describe using pip to setup and launch a jupyter lab instance. Conda
is obviously a choice as well.

## Steps
Open a terminal and change directories to this directory (INSTALL_DIR/splash-ml/examples)

Create a python environment:

`python3 -m venv env`

Activate your new environment

`source env/bin/activate`

Install the splash-ml project

`pip install -e ..`

Install requirements for the examples

'`ip install -r requirements-examples.txt`

Launch jupyter

`jupyter lab`


If all went well, a browser window should open with the examples folder open the directory view.