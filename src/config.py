"""Project Configuration"""
from .tools import EchoContainer, attrs_to_dict
from .xtypes import KwargsDict
from .tools import RelativeConfigPathGetter
from .logger import LoggerHub

import os
from configparser import ConfigParser
from sqlalchemy import types as SQLALCHEMYTYPE
from typing import Tuple


class CONST:
    c = 299792458  # speed of light in m/s


class PSQLTYPE:
    int = 'bigint'
    numeric = 'numeric'
    text = 'text'


class SRID:
    """Spatial reference IDs"""

    source = 4326  # lat,long srid used by source data (WGS84 CRS)
    eureka = 32616  # for March 25th Eureka track - UTM Zone 16N


class TABLE(EchoContainer):
    """Table names"""

    # ellipsis value means that the attribute value will echo the attribute name
    # but only when read using getattr()
    # e.g. my_attr = ... will be my_attr = 'my_attr'

    # input data
    asr_src = ...
    als_src = ...
    idc_src = ...
    mgn_src = ...
    esc30_src = ...
    pit_info = ...
    pit_dens = ...
    pit_salin = ...
    pit_strat = ...
    pit_temp = ...
    grid_zones = ...

    # extracted data
    asr_flags = ...

    # calculated observations
    ise_calc = ...

    # labeled asiras
    asr_grid_zone = ...

    # aggregating observations to ASIRAS footprints
    asr_refined = ...
    asr_zone = ...  # area of interest
    asr_fp = ...  # asiras footprints
    # observations clipped to area of interest
    als_clip = ...
    # observations aggregated to ASIRAS footprints
    asr_aggr_mgn = ...
    asr_aggr_als = ...
    asr_aggr_idc = ...
    asr_aggr_ise = ...
    asr_aggr = ...  # all observations combined
    # snow density interpolated to ASIRAS nadirs
    asr_snow_dens = ...

    # waveform processing
    asr_tfmra = ...
    asr_wshape = ...
    asr_wscaled = ...

    # sensor offset
    offset_samples = ...

    # retracker error
    asr_error = ...

    # summarized snow pits
    pit_summary = ...


# !!!! WARNING !!!!
# Class containers consumed to build DEFAULT.base_query_kwargs will have their
# attributes automatically applied to the Process.table_from_query method
# if the base_query_kwargs argument is not modified
#
# This means if an attribute name is changed in any of these classes, the
# change needs to be reflected in the query body.
# Fast way to do this is by replacing the entire placeholder across the project
#   e.g.
#   replace: {L@old_name} with: {L@new_name}
#
# Alternatively the changed attribute can be passed to the kwargs argument
# (the argument itself, not as individual keyword arguments) which will
# supercede base_query_kwargs


class COL(EchoContainer):
    """Column names"""
    # These specific columns have their names stored because they are
    # intermediate and may be used as inputs in future steps.
    # Class attributes are used to take advantage of IDE code inspections,
    # which validate that two steps using the same column will use the same
    # string when accessing the PostgreSQL database

    # geometry
    geom = ...
    latitude = ...
    longitude = ...

    # ID for each unique observation location
    id_asr = ...
    id_als = ...
    id_idc = ...
    id_mgn = ...
    id_esc30 = ...
    id_pit = ...
    # ID for each grid zone
    id_gzone = ...

    # labels
    grid_zone = ...

    # snow pit parameters
    hag_top = ...  # height above ground
    hag_bottom = ...
    hbs_top = ...  # height below snow surface
    hbs_bottom = ...
    pit_snow_depth = ...
    section_height = ...
    hag = ...
    hbs = ...
    # measurements
    snow_density = ...
    salinity_psu = ...
    gs1 = ...
    gs2 = ...
    gs3 = ...
    gl1 = ...
    gl2 = ...
    gl3 = ...
    grain_type = ...
    has_crust = ...
    has_slab = ...
    temp_c = ...

    # ASIRAS flight parameters
    # NOTE: changes need to be reflected with load.l1b.asiras_config
    retracker_range = ...
    velocity_xyz = ...
    # ASIRAS waveform parameters
    linear_scale_factor = ...
    power2_scale_factor = ...
    window_delay = ...
    ml_power_echo = ...
    altitude = ...

    # model parameters
    fp_size = ...  # footprint radius
    tfmra_threshold = ...  # TFMRA retracker threshold (proportion of peak)
    dens_adj = ...  # whether snow density is used to adjust the retracker
    offset_calib = ...  # which sensor offset calibration is used

    # observation measurements
    snow_elvtn = ...
    snow_depth = ...
    ice_elvtn = ...
    ice_deform = ...
    snow_rho_1 = ...
    snow_rho_2 = ...

    # aggregated observation measurements
    snow_depth_min = ...
    snow_depth_max = ...
    snow_depth_mean = ...
    snow_depth_stddev = ...
    snow_depth_count = ...
    snow_depth_rough = ...
    snow_elvtn_min = ...
    snow_elvtn_max = ...
    snow_elvtn_mean = ...
    snow_elvtn_stddev = ...
    snow_elvtn_count = ...
    snow_elvtn_rough = ...
    ice_deform_min = ...
    ice_deform_max = ...
    ice_deform_mean = ...
    ice_deform_stddev = ...
    ice_deform_count = ...
    ice_deform_rough = ...
    ice_elvtn_min = ...
    ice_elvtn_max = ...
    ice_elvtn_mean = ...
    ice_elvtn_stddev = ...
    ice_elvtn_count = ...
    ice_elvtn_rough = ...

    # interpolated snow density
    snow_dens_interp = ...
    dist_near = ...
    dist_far = ...

    # waveform processing results
    tfmra_elvtn = ...
    ppeak = ...
    ppeak_left = ...
    ppeak_right = ...
    rwidth = ...
    rwidth_left = ...
    rwidth_right = ...
    waveform_scaled = ...

    # ALS-ASIRAS sensor offset
    sensor_offset = ...

    # density and offset corrected retracked elevation and error
    adjust_elvtn = ...
    penetration = ...
    rel_penetration = ...
    error = ...
    abs_error = ...
    rel_error = ...
    abs_rel_error = ...
    above_snow = ...
    below_ice = ...
    in_snowpack = ...


class COLCONFIG:
    """Column configurations for table creation"""

    # SHP imports
    grid_zones = {
        COL.id_gzone: SQLALCHEMYTYPE.Integer,
        COL.grid_zone: SQLALCHEMYTYPE.Integer
    }

    idc = {
        COL.id_idc: SQLALCHEMYTYPE.Integer,
        COL.ice_deform: SQLALCHEMYTYPE.Integer
    }

    # CSV imports
    mgn = {
        COL.id_mgn: PSQLTYPE.int,
        COL.latitude: PSQLTYPE.numeric,
        COL.longitude: PSQLTYPE.numeric,
        COL.snow_depth: PSQLTYPE.numeric
    }

    esc30 = {
        COL.id_esc30: PSQLTYPE.int,
        COL.latitude: PSQLTYPE.numeric,
        COL.longitude: PSQLTYPE.numeric,
        COL.snow_rho_1: PSQLTYPE.numeric,
        COL.snow_rho_2: PSQLTYPE.numeric
    }

    pit_info = {
        COL.id_pit: PSQLTYPE.int,
        COL.latitude: PSQLTYPE.numeric,
        COL.longitude: PSQLTYPE.numeric
    }

    pit_obsv = {
        COL.id_pit: PSQLTYPE.int,
        COL.hag_top: PSQLTYPE.numeric,
        COL.hag_bottom: PSQLTYPE.numeric,
        COL.pit_snow_depth: PSQLTYPE.numeric,
        COL.hbs_top: PSQLTYPE.numeric,
        COL.hbs_bottom: PSQLTYPE.numeric,
        COL.section_height: PSQLTYPE.int,
    }

    pit_dens = {
        **pit_obsv, COL.snow_density: PSQLTYPE.numeric
    }

    pit_salin = {
        **pit_obsv, COL.salinity_psu: PSQLTYPE.numeric
    }

    pit_strat = {
        **pit_obsv,
        COL.gs1: PSQLTYPE.numeric,
        COL.gs2: PSQLTYPE.numeric,
        COL.gs3: PSQLTYPE.numeric,
        COL.gl1: PSQLTYPE.numeric,
        COL.gl2: PSQLTYPE.numeric,
        COL.gl3: PSQLTYPE.numeric,
        COL.grain_type: PSQLTYPE.text,
        COL.has_crust: PSQLTYPE.int,
        COL.has_slab: PSQLTYPE.int
    }

    pit_temp = {
        COL.id_pit: PSQLTYPE.int,
        COL.hag: PSQLTYPE.numeric,
        COL.pit_snow_depth: PSQLTYPE.numeric,
        COL.hbs: PSQLTYPE.numeric,
        COL.temp_c: PSQLTYPE.numeric
    }

    # maintain compatibility with previous version for comparison
    _id = 'int8'
    _num = 'float8'
    _arr = '_float8'

    # Retracking Tables
    asr_tfmra = {
        COL.id_asr: _id,
        COL.tfmra_threshold: _num,
        COL.tfmra_elvtn: _num
    }

    asr_wscaled = {
        COL.id_asr: _id,
        COL.waveform_scaled: '_' + _num
    }

    asr_wshape = {
        COL.id_asr: _id,
        COL.rwidth: _num,
        COL.rwidth_left: _num,
        COL.rwidth_right: _num,
        COL.ppeak: _num,
        COL.ppeak_left: _num,
        COL.ppeak_right: _num
    }


class FUNC(EchoContainer):
    """Helper functions"""
    # calculate speed of light through snow based on density coefficient
    c_snow_dens_coeff = ...
    # adjust dominant scattering interface (TFMRA retracked ice surface
    # elevation) based on snow density
    adjust_dsi_by_snow_dens = ...


class PARAM:
    """Processing parameters"""

    # ASIRAS RADAR SPECIFICATIONS
    # DOI: 10.1109/IGARSS.2004.1369792
    freq = 13.5e9  # 13.5 GHz Ku band
    # w=f/v -> wavelength (m) = frequency / speed of light
    wlength = freq / CONST.c
    bwidth = 1e9  # 1 GHz bandwidth
    plength = 1 / bwidth  # compressed pulse length in seconds
    n_avg = 64  # number of the product pre-sums (averages)
    # pulse repetition frequency
    # this is supposed to come from the ASIRAS instrument configuration flags
    # but Veit Helm said this is 2500 in the log files
    prf = 2500

    # ASIRAS RADAR FOOTPRINTS
    # Pulse-doppler limited radar footprint
    pdlf_key = -1  # proxy radius key to differentiate it from circular ones
    # Circular footprints
    # will also consider circular footprints with these radii
    # max radius is ~40m to capture all observation data
    fp_radii = list(range(6, 41, 2))

    # OBSERVATION AGGREGATION
    # percentile margin to use for calculating measurement roughness
    # (h-topo for snow surface/ALS)
    rough_margin = 0.05

    # FILTERING ASIRAS RECORDS
    # removing radar points that are suboptimal
    # maximum roll and pitch deviation from nadir
    roll_dev = 1.5
    pitch_dev = 1.5
    # larget aggregation radius
    # (exclude where the nearest snow depth observation is further than this)
    max_fp_radius = max(fp_radii)
    # minimum number of snow depth observations within the radius above
    near_points = 10

    # WAVEFORM PROCESSING
    bin_size = 0.109787  # size of each ASIRAS bin in meters
    # retracker waveform using TFMRA with these thresholds
    retracker_thresholds = [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    # pulse peakiness configuration
    ppeak_indices_left = [-3, -2, -1]
    ppeak_indices_right = [1, 2, 3]
    # minimum proportion of peak considered to be signal
    signal_threshold = 0.01

    # SENSOR OFFSET SAMPLING
    # offset sampling WHERE statements
    offset_calib_params = dict(
        main=[
            'fp_size=-1',
            # footprint size for aggregating surface measurements
            'snow_depth_count>=3',  # minimum number of observation points
            'snow_elvtn_count>=3',
            'snow_depth_mean<=0.10',  # maximum average snow depth
            'ice_deform_max<=0',  # highest allowable maximum ice deform
            'ocog_width>0'  # excludes erroneous returns
        ]
    )


signal_threshold = 0.01


class DEFAULT:
    """Processing defaults/fallback values"""

    # NOTE: there is a second pair of these for the Session class
    # found in postgis.config.DEFAULT
    # I couldn't find a stable way to link these two while providing a config
    # file to the user and without requiring Session to always have this pair
    # supplied
    schema = "public"  # last fallback schema
    geom_col = "geom"  # last fallback geom col

    # combine attributes in these classes
    # TABLE not included so that process methods have the table references
    # in their function call arguments so it's clearer which steps use which
    # tables
    base_query_kwargs = attrs_to_dict(COL, PARAM, FUNC)


def read_config(
        config_path: str
) -> Tuple[RelativeConfigPathGetter, KwargsDict, KwargsDict]:

    if not os.path.isfile(config_path):
        raise FileNotFoundError(
            f"Config file not found at '{config_path}' with current working "
            f"directory {os.getcwd()}"
        )
    else:
        config_parser = ConfigParser()
        config_parser.read(config_path)

    # build file path getter
    filepath = RelativeConfigPathGetter(
        config_parser['Files'],
        'data_dir'
    )

    # build logging context management object
    logger = config_parser['Logger']
    logger_hub = LoggerHub(**logger)

    # build database configuration
    db = config_parser['Database']
    new_session_kwargs = dict(
        host=db['host'],
        port=db['port'],
        dbname=db['dbname'],
        user=db['user'],
        password=db['password'],
        session_kwargs=dict(
            default_schema=db['default_schema'],
            default_geom_col=db['default_geom_col']
        ),
        logger_hub=logger_hub
    )

    new_process_kwargs = dict(
        name='methods',
        logger_hub=logger_hub
    )

    return filepath, new_session_kwargs, new_process_kwargs
