import sys

from .config import read_config, TABLE, COL, COLCONFIG, SRID, PARAM
from .postgis import new_session
from .process import new_process, queries


def main():

    try:
        config_path = sys.argv[1]
    except IndexError:
        raise SystemError("no config.ini path argument supplied")

    filepath, new_session_kwargs, new_process_kwargs = read_config(config_path)

    drop_obsolete = False

    with new_session(**new_session_kwargs) as session, \
            new_process(session=session, **new_process_kwargs) as process:

        # setup necessary helper functions
        # NOTE: if the function name interfere with existing names, they can be
        # modified in config.FUNC
        process.execute_query(
            "Setup", queries.setup
        )

        # load input data
        # CryoVEX Airborne Observations - L1B format
        process.load_asiras(
            filepath.get('asr'), TABLE.asr_src, COL.id_asr, COL.longitude,
            COL.latitude,
            SRID.source, SRID.eureka
        )
        process.load_als(
            filepath.get('als'), TABLE.als_src, COL.id_als, COL.snow_elvtn,
            SRID.eureka,
        )
        # grid zone areas
        process.load_shp(
            "Grid Zones", filepath.get('grid_zones'), TABLE.grid_zones,
            COLCONFIG.grid_zones, COL.id_gzone,
        )
        # Ice deformed classication from King et al. 2015
        process.load_shp(
            "Ice Deformed Class", filepath.get('idc'), TABLE.idc_src,
            COLCONFIG.idc, COL.id_idc
        )
        # Eureka ground measurements
        process.load_csv_with_xy(
            "Magnaprobe", filepath.get('mgn'), TABLE.mgn_src, COLCONFIG.mgn,
            COL.longitude, COL.latitude, SRID.source, SRID.eureka,
            COL.id_mgn
        )
        process.load_csv_with_xy(
            "ESC-30", filepath.get('esc30'), TABLE.esc30_src, COLCONFIG.esc30,
            COL.longitude, COL.latitude, SRID.source, SRID.eureka,
            COL.id_esc30
        )
        # Eureka snow pits
        # info table with spatial coordinates
        process.load_csv_with_xy(
            "Snow Pit Info", filepath.get('pit_info'), TABLE.pit_info,
            COLCONFIG.pit_info,
            COL.longitude, COL.latitude, SRID.source, SRID.eureka,
            COL.id_pit
        )
        # measurement tables
        for path, name, table, col_config in zip(
                [
                    filepath.get('pit_dens'), filepath.get('pit_salin'),
                    filepath.get('pit_strat'), filepath.get('pit_temp')
                ],
                ["Density", "Salinity", "Stratigraphy", "Temperature"],
                [
                    TABLE.pit_dens, TABLE.pit_salin, TABLE.pit_strat,
                    TABLE.pit_temp
                ],
                [
                    COLCONFIG.pit_dens, COLCONFIG.pit_salin,
                    COLCONFIG.pit_strat, COLCONFIG.pit_temp
                ]
        ):
            process.load_csv("Snow Pit" + name, path, table, col_config)

        # label ASIRAS ids with their grid zone
        process.create_table_from_query(
            "ASIRAS grid zone", TABLE.asr_grid_zone, queries.label_grid_zones,
            kwargs=dict(
                src=TABLE.asr_src, src_id=COL.id_asr,
                zones=TABLE.grid_zones
            )
        )

        # calculate ice surface elevation
        process.create_table_from_query(
            "Ice Surface Elvtn.", TABLE.ise_calc, queries.elvtn_bottom,
            kwargs=dict(mgn=TABLE.mgn_src, als=TABLE.als_src),
            spatial_index=True, primary_key_cols=COL.id_mgn
        )

        # refine asiras points to remove inaccurate records
        process.create_table_from_query(
            "Refine ASIRAS", TABLE.asr_refined, queries.refine_asr,
            kwargs=dict(asr=TABLE.asr_src, obs=TABLE.mgn_src),
            spatial_index=True, primary_key_cols=COL.id_asr
        )

        # create asiras footprints
        process.create_asiras_footprints(
            TABLE.asr_fp, TABLE.asr_src, TABLE.asr_refined
        )

        # aggregate each input to footprints
        aggr_query = queries.aggregate_observations
        aggr_index_cols = (COL.id_asr, COL.fp_size)
        if not session.table_exists(TABLE.asr_aggr):
            # create buffer zone of filtered ASIRAS points to filter
            # observations
            process.create_table_from_query(
                "ASIRAS Zone", TABLE.asr_zone, queries.buffer_zone,
                kwargs=dict(points=TABLE.asr_refined, dist=PARAM.max_fp_radius),
                spatial_index=True
            )

            # clip ALS to ASIRAS buffer zone
            # NOTE: this significantly reduces the runtime
            # (from 1hr30min to 20min)
            # depending on machine
            process.create_table_from_query(
                "Clip ALS", TABLE.als_clip, queries.select_intersect,
                kwargs=dict(a=TABLE.als_src, b=TABLE.asr_zone),
                spatial_index=True, primary_key_cols=COL.id_als
            )

            process.create_table_from_query(
                "Aggr. Magnaprobe", TABLE.asr_aggr_mgn,
                aggr_query,
                kwargs=dict(
                    ftpr=TABLE.asr_fp, obsv=TABLE.mgn_src,
                    val=COL.snow_depth,
                    val_min=COL.snow_depth_min,
                    val_max=COL.snow_depth_max,
                    val_mean=COL.snow_depth_mean,
                    val_stddev=COL.snow_depth_stddev,
                    val_count=COL.snow_depth_count,
                    val_rough=COL.snow_depth_rough
                ),
                simple_index_cols=aggr_index_cols
            )
            process.create_table_from_query(
                "Aggr. ALS", TABLE.asr_aggr_als,
                aggr_query,
                kwargs=dict(
                    ftpr=TABLE.asr_fp, obsv=TABLE.als_clip,
                    val=COL.snow_elvtn,
                    val_min=COL.snow_elvtn_min,
                    val_max=COL.snow_elvtn_max,
                    val_mean=COL.snow_elvtn_mean,
                    val_stddev=COL.snow_elvtn_stddev,
                    val_count=COL.snow_elvtn_count,
                    val_rough=COL.snow_elvtn_rough
                ),
                simple_index_cols=aggr_index_cols
            )
            process.create_table_from_query(
                "Aggr. Ice Def. Class.", TABLE.asr_aggr_idc,
                aggr_query,
                kwargs=dict(
                    ftpr=TABLE.asr_fp, obsv=TABLE.idc_src,
                    val=COL.ice_deform,
                    val_min=COL.ice_deform_min,
                    val_max=COL.ice_deform_max,
                    val_mean=COL.ice_deform_mean,
                    val_stddev=COL.ice_deform_stddev,
                    val_count=COL.ice_deform_count,
                    val_rough=COL.ice_deform_rough
                ),
                simple_index_cols=aggr_index_cols
            )
            process.create_table_from_query(
                "Aggr. Ice Surf. Elv.", TABLE.asr_aggr_ise,
                aggr_query,
                kwargs=dict(
                    ftpr=TABLE.asr_fp, obsv=TABLE.ise_calc,
                    val=COL.ice_elvtn,
                    val_min=COL.ice_elvtn_min,
                    val_max=COL.ice_elvtn_max,
                    val_mean=COL.ice_elvtn_mean,
                    val_stddev=COL.ice_elvtn_stddev,
                    val_count=COL.ice_elvtn_count,
                    val_rough=COL.ice_elvtn_rough
                ),
                simple_index_cols=aggr_index_cols
            )

            # combine aggregated measurements
            process.combine_aggregated_measurements(
                TABLE.asr_aggr,
                [
                    TABLE.asr_aggr_mgn, TABLE.asr_aggr_als,
                    TABLE.asr_aggr_idc, TABLE.asr_aggr_ise
                ],
                [COL.id_asr, COL.fp_size],
                [COL.id_asr, COL.fp_size]
            )

        # drop obsolete aggregation tables if they still exist
        if drop_obsolete and session.table_exists(TABLE.asr_aggr):
            process.drop_table_if_exist(
                TABLE.asr_aggr_mgn, TABLE.asr_aggr_als, TABLE.asr_aggr_idc,
                TABLE.asr_aggr_ise,
                TABLE.asr_zone, TABLE.als_clip  # ALS clipping
            )

        # interpolate snow density from ESC30 for each asiras point
        process.create_table_from_query(
            "Interp. Snow Dens.", TABLE.asr_snow_dens,
            queries.interpolate_snow_density,
            kwargs=dict(asr=TABLE.asr_src, dens=TABLE.esc30_src),
            primary_key_cols=COL.id_asr
        )

        # apply TFMRA retracker to ASIRAS waveforms
        process.waveform_processing(
            TABLE.asr_tfmra, COLCONFIG.asr_tfmra,
            TABLE.asr_wshape, COLCONFIG.asr_wshape,
            TABLE.asr_wscaled, COLCONFIG.asr_wscaled,
            TABLE.asr_src
        )

        process.offset_calibration(
            TABLE.offset_samples, PARAM.offset_calib_params, dict(
                tfmra=TABLE.asr_tfmra, src=TABLE.asr_src, aggr=TABLE.asr_aggr,
                snow_dens=TABLE.asr_snow_dens, grid_zone=TABLE.asr_grid_zone
            )
        )

        # calculate ice surface estimate error
        process.create_table_from_query(
            "Retracker Error", TABLE.asr_error, queries.retracker_error,
            kwargs=dict(
                offset=TABLE.offset_samples,
                tfmra=TABLE.asr_tfmra,
                aggr=TABLE.asr_aggr,
                snow_dens=TABLE.asr_snow_dens
            ),
            simple_index_cols=[
                COL.id_asr, COL.fp_size, COL.tfmra_threshold, COL.dens_adj
            ]
        )

        # summarize snow pit observations
        process.create_table_from_query(
            "Summarize Snow Pits", TABLE.pit_summary, queries.summarize_pits,
            kwargs=dict(
                pit_salin=TABLE.pit_salin,
                pit_strat=TABLE.pit_strat,
                pit_dens=TABLE.pit_dens,
                pit_temp=TABLE.pit_temp,
                pit_info=TABLE.pit_info
            )
        )


if __name__ == "__main__":
    main()
