# Cryovex Eureka 2014

Evaluating Ice Surface Elevation Estimates using Airborne Radar Altimetry from the CryoVEX-Eureka 2014 Arctic Campaign

**Author:** Paul Donchenko

**Special Thanks to:** 

​    Josh King (Environment and Climate Change Canada)

​    Richard Kelly (University of Waterloo)

## Installation

Clone this repository to a directory of your choice.

### Anaconda 3 Environment

1. Install Anaconda 3 from https://repo.anaconda.com/archive/ or https://repo.continuum.io/archive/

   Preferably version `2019.10` for Windows 10, although future versions on other platforms are likely to work as well.

2. Create a new conda environment from the supplied `cveureka.yml` file by running 
   `conda env create -f "<path_to_project>/cveureka.yml"`

   This should create a new environment in your `Anaconda3/envs` directory called `cveureka`. The environment will contain Python 3.7 and all the necessary packages.

   If you have issues creating the environment, try switching to conda version `4.7.12`

### PostgreSQL Database

1. Download PostgreSQL for your platform from https://www.postgresql.org/download/

   Alternatively you can use a remote PostgreSQL connection.

   This project was developed on version 10, so your version must be equal or greater. If you have issues with deprecated features, try using version 10.

2. Install PostGIS in the target database. Steps will vary depending on platform https://postgis.net/install/

### Configuration

1. Modify the `Database` section of `config.ini` to match the connection settings of your PostgreSQL database.

   `default_schema` can be modified if there is existing data in the `public` schema that may cause name collisions.

   `default_geom_col` should not be modified unless output tables will be inputs into a pipeline

2. Modify the `data_dir` variable in the `Files` section to match the location of the `data` folder which contains all of the input datasets needed to run the methods procedure. By default the folder is located inside the root repository directory.

   The individual dataset variables do not have to be modified and should sit inside the `data` folder.

   Due to their size, the input datasets are not available in this repository. Contact the author for details on how to obtain them.

## Running the Method Procedure

The `src/method.py` script is responsible for taking the input data and producing output tables in the PostgreSQL database which have ice surface estimates and their associated error.

To run the method procedure, activate the `cveureka` conda environment, and then run the `method.py` script as module `src.method` with the activated python environment. The path to the `config.ini` should be the first and only argument to the script. The script must be a run as a module due to the use of relative imports.

To run in Windows, use the following commands with the repository root folder as the working directory:

```bash
conda activate
python -m src.example "config.ini"

```

A batch file `method.bat` is provided with default configuration for running in Windows.

## Documentation

A description of the L1B binary format used to store the airborne ALS and ASIRAS data is available in `docs/cryovex_airborne_data_description.pdf`

Descriptions for output tables and columns can be found in `docs/table_info.md`

