# Cryovex Eureka 2014

Evaluating Ice Surface Elevation Estimates using Airborne Radar Altimetry from the CryoVEX-Eureka 2014 Arctic Campaign

**Author:** Paul Donchenko

**Special Thanks to:** 

​    Josh King (Environment and Climate Change Canada)

​    Richard Kelly (University of Waterloo)

## Installation

Clone this repository to a directory of your choice.

### Airborne L1B Data

Most of the data used in this analysis is located in the `data` folder. The L1B ASIRAS and ALS airborne data from the CryoVEx 2014 campaign is not provided.

To obtain this data, contact ESA and request the datasets listed below. Information about how to request data can be found at: https://earth.esa.int/web/guest/pi-community/apply-for-data/campaigns

or by its DOI: https://doi.org/10.5270/esa-aa4xtkn

By default, both files should be placed into `data/l1b`

#### ASIRAS
* **File Name**: `AS3OA03_ASIWL1B040320140325T160941_20140325T164233_0001.DBL`
* **Date**: 2014/03/25
* **Product**: L1B

#### ALS
* **File Name**: `ALS_L1B_20140325T160930_164957`
* **Date**: 2014/03/25
* **Product**: L1B

### ECCC 2014 Ground Observations Data

A modified version of this dataset is included in the repository. The original can be found at the DOI: https://doi.org/10.5281/zenodo.823679

### Anaconda 3 Environment

1. Install Anaconda 3 from https://repo.anaconda.com/archive/ or https://repo.continuum.io/archive/

   Preferably version `2019.10` for Windows 10, although future versions on other platforms are likely to work as well.

2. Create a new conda environment from the supplied `cveureka.yml` file by running 
   `conda env create -f "<path_to_project>/cveureka.yml"` from the Anaconda prompt

   This should create a new environment in your `Anaconda3/envs` directory called `cveureka`. The environment will contain Python 3.7 and all the necessary packages.

   If you have issues creating the environment, try switching to conda version `4.7.12`

### PostgreSQL Database

1. Download PostgreSQL for your platform from https://www.postgresql.org/download/

   Alternatively you can use a remote PostgreSQL connection.

   This project was developed on version 10, so your version must be equal or greater. If you have issues with deprecated features, try using version 10.
   
2. Create a new target database if one doesn't exist

3. Install PostGIS in the target database. Steps will vary depending on platform https://postgis.net/install/

### Configuration

1. Modify the `Database` section of `config.ini` to match the connection settings of your PostgreSQL database.

   `default_schema` should be changed to a schema specially prepared for this project. Using the `public` schema is not recommended since that is where PostGIS installs its functions, and moving the results tables after they are created can be tricky.

   `default_geom_col` should not be modified unless output tables will be inputs into a pipeline

2. Modify the `data_dir` variable in the `Files` section to match the location of the `data` folder which contains all of the input datasets needed to run the methods procedure. By default the folder is located inside the root repository directory.

   The individual dataset variables do not have to be modified and should sit inside the `data` folder.

## Usage

The data is process in two parts: the method and the analysis. The method takes the raw input and produces PostgreSQL tables with the ice surface estimate and error results, which is equivalent to the manuscript **Methods**  and **Results** section. The analysis reshapes parts of the results to create figures that are referenced in the **Analysis** and **Discussion** manuscript sections.

### Method

The `src/method.py` script is responsible for taking the input data and producing output tables in the PostgreSQL database which have ice surface estimates and their associated error.

To run the method procedure, activate the `cveureka` conda environment, and then run the `method.py` script as module `src.method` with the activated python environment. The path to the `config.ini` should be the first and only argument to the script. The script must be a run as a module due to the use of relative imports.

To run in Windows, use the following commands with the repository root folder as the working directory:

```bash
conda activate
python -m src.example "config.ini"
```

A batch file `method.bat` is provided with default configuration for running in Windows.

### Analysis

The `src/cve_analysis` directory contains  [R scripts](https://www.r-project.org/) that connect to the PostgreSQL database, consume the results and produce the analysis figures:

* `config.r` stores processing constants and reads configurations from `config.ini` in the project root
* `tools.r` contains helper functions for reshape and analyzing the results
* scripts that begin with `plot_`  generate the manuscript plots into the `plots` directory in the project root

None of the scripts need to modified to produce the default results. If `config.ini` cannot be found the process will ask for its location.

It is recommended to use [RStudio](https://rstudio.com/) to run the scripts as it should retrieve and install the necessary packages automatically.

Run the `plot_err_all.r` script to produce all plots.

## Documentation

A description of the L1B binary format used to store the airborne ALS and ASIRAS data is available in `docs/cryovex_airborne_data_description.pdf`

Descriptions for output tables and columns can be found in `docs/table_info.md`

