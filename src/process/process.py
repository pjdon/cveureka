from typing import Optional, ContextManager, List, Iterable, Dict
from contextlib import contextmanager
from datetime import datetime
import numpy as np
import pandas as pd
import logging
from psycopg2 import sql as pgs

from . import queries
from ..xtypes import KwargsDict
from ..postgis.xtypes import Table, IndexColumns, OmniColumns, Query, \
    ColumnConfigDict
from ..config import DEFAULT, COL, PARAM
from ..logger import ContextLoggable, empty_logger_hub
from ..postgis import Session, ResultFormat
from ..postgis.tools import \
    parse_rows_to_sql_values, \
    sql_block_where, \
    stack_sql_lines, \
    union_sql_blocks, \
    column_config_dict_to_list
from ..load.l1b import AsirasLoader, AlsLoader
from .tools import \
    lin_interp_from_first_max, \
    InterpolateDirection, \
    calc_scaled_waveform, \
    calc_tfmra_elevation, \
    calc_first_bin_elvtn, \
    aggregate_relative_indices


class Process:

    def __init__(
            self,
            name: str,
            session: Session,
            logger_hub=empty_logger_hub(),
    ):
        self.name = name
        self.session = session
        self.logger_hub = logger_hub

    def context_logger(self, context_name: str) -> logging.Logger:
        return self.logger_hub.context(context_name)

    @staticmethod
    def _log_table_exists(
            logger: ContextLoggable, table_name: str
    ):
        logger.debug(
            f"Table {table_name} already exists. Skipping step."
        )

    @staticmethod
    def _log_table_has_columns(
            logger: ContextLoggable, table_name: str, column_names: List[str]
    ):
        logger.debug(
            f"Table {table_name} already has column(s) {column_names}. "
            f"Skipping step."
        )

    def _simple_index_on_cols(
            self,
            logger: ContextLoggable,
            table_name: Table,
            columns: List[str]
    ):
        if columns:
            logger.info(f"creating index on columns{columns}")
            self.session.create_simple_index(table_name, columns)

    def drop_table_if_exist(
            self,
            *tables: Table
    ):
        logger = self.context_logger("Drop Table")
        for table in tables:
            if self.session.table_exists(table):
                self.session.drop_table(table)
                self.session.commit()
                logger.info(f"Dropped {table}")
            else:
                logger.debug(f"{table} already doesn't exist")

    def create_geom_col_from_xy(
            self,
            logger: ContextLoggable,
            output_table: Table,
            col_x_name: str,
            col_y_name: str,
            srid_input: int,
            srid_output: int,
            col_geom_name=COL.geom,
            geom_dim=2,
            geom_type="POINT"
    ):
        # create a geometry column from x,y if it hasn't been created
        if self.session.table_has_none_of_cols(output_table, col_geom_name):
            logger.info(
                f"Creating geometry column "
                f" from {col_x_name}, {col_y_name} into {col_geom_name}"
            )
            self.session.add_geom_col_from_xy(
                output_table, col_x_name, col_y_name, srid_input, geom_type,
                geom_dim, srid_output, col_geom_name
            )
            logger.info(
                f"Creating spatial index on columns {col_geom_name}"
            )
            self.session.create_spatial_index(output_table, col_geom_name)
            self.session.commit()

        else:
            self._log_table_has_columns(logger, output_table, col_geom_name)

    def format_query_with_base_args(
            self,
            query: str,
            main_kwargs: Optional[KwargsDict] = None,
            base_query_kwargs: Optional[KwargsDict] = None,
            fallback_base_query_kwargs=DEFAULT.base_query_kwargs
    ) -> pgs.Composable:
        format_kwargs = {
            **(base_query_kwargs or fallback_base_query_kwargs),
            **(main_kwargs or {})
        }
        return self.session.format_query(query, None, format_kwargs)

    def create_table_from_query(
            self,
            context_name: str,
            output_table: Table,
            query: str,
            kwargs: Optional[KwargsDict] = None,
            msg: Optional[str] = None,
            spatial_index=False,
            primary_key_cols: Optional[OmniColumns] = None,
            simple_index_cols: Optional[OmniColumns] = None,
            autocommit=True,
            temp=False,
            base_query_kwargs: Optional[KwargsDict] = None
    ):
        logger = self.context_logger(context_name)
        if msg:
            logger.info(msg)

        if not self.session.table_exists(output_table):

            formatted_query = self.format_query_with_base_args(
                query, kwargs, base_query_kwargs
            )

            logger.info(f"Creating table {output_table}")
            self.session.create_table_as(output_table, formatted_query, temp)

            if spatial_index:
                logger.info(
                    f"Creating spatial index on default geometry column"
                )
                self.session.create_spatial_index(output_table)

            if primary_key_cols:
                logger.info(
                    f"Setting primary key to {primary_key_cols}"
                )
                self.session.set_primary_key(output_table, primary_key_cols)

            if simple_index_cols:
                logger.info(
                    f"Creating simple index on columns {simple_index_cols}"
                )
                self.session.create_simple_index(
                    output_table,
                    simple_index_cols
                )

            if autocommit:
                self.session.commit()
        else:
            self._log_table_exists(logger, output_table)

    def execute_query(
            self,
            context_name: str,
            query: str,
            kwargs: Optional[KwargsDict] = None,
            msg: Optional[str] = None,
            autocommit=True,
            base_query_kwargs: Optional[KwargsDict] = None

    ):
        logger = self.context_logger(context_name)
        if msg:
            logger.info(msg)

        formatted_query = self.format_query_with_base_args(
            query, kwargs, base_query_kwargs
        )

        self.session.execute_query(formatted_query)

        if autocommit:
            self.session.commit()

    def load_asiras(
            self,
            file_path: str, output_table: Table,
            col_id_name: str,
            col_x_name: str,
            col_y_name: str,
            srid_input: int,
            srid_output: int,
            col_geom_name=COL.geom,
            loader_kwargs: Optional[KwargsDict] = None,
            extract_kwargs: Optional[KwargsDict] = None
    ):
        logger = self.context_logger('Load ASIRAS')

        # load ASIRAS to table if it hasn't been created
        if not self.session.table_exists(output_table):
            loader = AsirasLoader(
                logger, **(loader_kwargs or {})
            )
            logger.info(
                f"Extracting ASIRAS data to table {output_table} from "
                f"{file_path}"
            )
            loader.extract_to_database(
                self.session, file_path, output_table, col_id_name,
                **(extract_kwargs or {})
            )
            self.session.commit()
        else:
            self._log_table_exists(logger, output_table)

        # create a geometry column from x,y if it hasn't been created
        self.create_geom_col_from_xy(
            logger, output_table, col_x_name, col_y_name,
            srid_input, srid_output, col_geom_name
        )

    def load_als(
            self,
            file_path: str, output_table: Table,
            col_id_name: str,
            col_elvtn_name: str,
            output_srid: int,
            col_geom_name=COL.geom,
            extract_kwargs: Optional[KwargsDict] = None
    ):
        logger = self.context_logger('Load ALS')

        # load ALS to table if it hasn't been created
        if not self.session.table_exists(output_table):
            loader = AlsLoader(logger)
            logger.info(
                f"Extracting ALS data to table {output_table} from {file_path}"
            )
            loader.extract_to_database(
                self.session, file_path, output_table,
                col_id_name, col_elvtn_name, col_geom_name, output_srid,
                **(extract_kwargs or {})
            )
            logger.info(
                f"Creating spatial index on columns {col_geom_name}"
            )
            self.session.create_spatial_index(output_table, col_geom_name)
            self.session.commit()
        else:
            self._log_table_exists(logger, output_table)

    def load_csv(
            self,
            dataset_name: str,
            file_path: str,
            output_table: Table,
            column_config: ColumnConfigDict,
            primary_key_cols: Optional[OmniColumns] = None,
            create_kwargs: Optional[KwargsDict] = None,
            read_csv_kwargs: Optional[KwargsDict] = None
    ):
        logger = self.context_logger("Load " + dataset_name)

        # load CSV to table if it hasn't been created
        if not self.session.table_exists(output_table):
            logger.info(
                f"Extracting CSV into table {output_table} from {file_path}"
            )
            self.session.create_table_from_csv(
                output_table, column_config, file_path,
                read_csv_kwargs=read_csv_kwargs,
                **(create_kwargs or {})
            )

            if primary_key_cols:
                self.session.set_primary_key(output_table, primary_key_cols)

            self.session.commit()
        else:
            self._log_table_exists(logger, output_table)

    def load_csv_with_xy(
            self,
            dataset_name: str,
            file_path: str,
            output_table: Table,
            column_config: ColumnConfigDict,
            col_x_name: str,
            col_y_name: str,
            srid_input: int,
            srid_output: int,
            primary_key_col: Optional[OmniColumns] = None,
            col_geom_name=COL.geom,
            create_kwargs: Optional[KwargsDict] = None,
            read_csv_kwargs: Optional[KwargsDict] = None,
    ):
        logger = self.context_logger("Load " + dataset_name)

        self.load_csv(
            dataset_name, file_path, output_table, column_config,
            primary_key_col, create_kwargs, read_csv_kwargs
        )

        # create a geometry column from x,y if it hasn't been created
        self.create_geom_col_from_xy(
            logger, output_table, col_x_name, col_y_name,
            srid_input, srid_output, col_geom_name
        )

    def load_shp(
            self,
            dataset_name: str,
            file_path: str, output_table: Table,
            column_config: ColumnConfigDict,
            primary_key_col: IndexColumns,
            col_geom_name=COL.geom,
            read_file_kwargs: Optional[KwargsDict] = None,
            to_sql_kwargs: Optional[KwargsDict] = None,
            connection_kwargs: Optional[KwargsDict] = None
    ):
        logger = self.context_logger("Load " + dataset_name)

        # load SHP to table if it hasn't been created
        if not self.session.table_exists(output_table):
            logger.info(
                f"Extracting SHP into table {output_table} from {file_path}"
            )
            self.session.create_table_from_shp(
                output_table, file_path, column_config,
                sql_geom_col_name=col_geom_name,
                read_file_kwargs=read_file_kwargs,
                to_sql_kwargs=to_sql_kwargs,
                connection_kwargs=connection_kwargs
            )
            self.session.set_primary_key(output_table, primary_key_col)
            self.session.commit()
        else:
            self._log_table_exists(logger, output_table)

    def create_asiras_footprints(
            self,
            output_table: Table,
            asiras_points_table: Table,
            asiras_refined_table: Table,
            simple_index_columns=(COL.id_asr, COL.fp_size),
            footprint_radii=PARAM.fp_radii,
            query=queries.asr_footprints,
            query_kwargs: Optional[KwargsDict] = None,
            spatial_index=True
    ):
        rows = [[i] for i in footprint_radii]

        kwargs = {
            'asr': asiras_points_table,
            'asr_refined': asiras_refined_table,
            'fp_radii_values': parse_rows_to_sql_values(rows),
            **(query_kwargs or {})
        }

        self.create_table_from_query(
            'ASIRAS footprints', output_table, query,
            kwargs,
            spatial_index=spatial_index,
            simple_index_cols=simple_index_columns
        )

    def combine_aggregated_measurements(
            self,
            out_table: Table,
            join_tables: List[Table],
            join_columns: List[str],
            simple_index_columns: List[str]
    ):
        logger = self.context_logger("Combine Aggr.")

        if self.session.table_exists(out_table):
            self._log_table_exists(logger, out_table)
            return

        logger.info(f"Creating table {out_table}")
        self.session.create_table_by_join_using(
            out_table, join_tables, join_columns
        )

        self._simple_index_on_cols(logger, out_table, simple_index_columns)

    def waveform_processing(
            self,
            tfmra_table: Table,
            tfmra_col_config: ColumnConfigDict,
            wshape_table: Table,
            wshape_col_config: ColumnConfigDict,
            wscaled_table: Table,
            wscaled_col_config: ColumnConfigDict,
            waveform_table: Table,
            retracker_thresholds=PARAM.retracker_thresholds,
            ppeak_indices_left=PARAM.ppeak_indices_left,
            ppeak_indices_right=PARAM.ppeak_indices_right,
            signal_threshold=PARAM.signal_threshold,
            bin_size=PARAM.bin_size,
            waveform_id_col=COL.id_asr,
            linear_scale_factor_col=COL.linear_scale_factor,
            power2_scale_factor_col=COL.power2_scale_factor,
            rwc_delay_col=COL.window_delay,
            sensor_elvtn_col=COL.altitude,
            waveform_col=COL.ml_power_echo,
            tfmra_simple_index_cols=(COL.id_asr, COL.tfmra_threshold),
            wshape_simple_index_cols=(COL.id_asr,),
            wscaled_simple_index_cols=(COL.id_asr,)
    ):
        logger = self.context_logger("Waveform Processing")

        output_tables = [
            tfmra_table,
            wshape_table,
            wscaled_table
        ]

        output_tables_exist = [
            self.session.table_exists(t) for t in output_tables
        ]

        # skip if all tables exist
        if not output_tables_exist or all(output_tables_exist):
            logger.debug(f"all output table exist: {output_tables}")
            return

        logger.info(f"reading data from {waveform_table}")

        cols_to_read = [
            waveform_id_col, linear_scale_factor_col, power2_scale_factor_col,
            rwc_delay_col, sensor_elvtn_col, waveform_col
        ]

        read_cols = self.session.select(
            waveform_table, cols_to_read, result_format=ResultFormat.LIST,
            as_cols=True
        )

        # read the waveform_id column as integer and the rest as numpy floats
        waveform_id = np.array(read_cols[0], dtype=int)
        # each row (item) in attribute is a single ASIRAS observation
        # each column in waveform is a range bin
        lin_factor, pow2_factor, rwc_delay, sensor_elvtn, waveform = \
            [np.array(col, dtype=float) for col in read_cols[1:]]

        # number of range bins in waveform
        num_bins = waveform.shape[1]

        # scale waveform to remove effects of gains and attenuations
        scaled_waveform = calc_scaled_waveform(
            waveform, lin_factor, pow2_factor, logger
        )

        # elevation of first range bin
        first_bin_elvtn = calc_first_bin_elvtn(
            bin_size, rwc_delay, sensor_elvtn, num_bins, logger
        )

        self.create_tfmra_table(
            tfmra_table, tfmra_col_config,
            scaled_waveform, first_bin_elvtn, waveform_id,
            retracker_thresholds, bin_size,
            tfmra_simple_index_cols
        )

        self.create_wshape_table(
            wshape_table, wshape_col_config,
            scaled_waveform, waveform_id,
            ppeak_indices_left, ppeak_indices_right, bin_size, signal_threshold,
            wshape_simple_index_cols
        )

        self.create_wscaled_table(
            wscaled_table, wscaled_col_config,
            waveform_id, scaled_waveform,
            wscaled_simple_index_cols
        )

    def create_tfmra_table(
            self,
            out_table: Table,
            col_config: ColumnConfigDict,
            scaled_waveform: np.ndarray,
            first_bin_elvtn: np.ndarray,
            waveform_id: np.ndarray,
            retracker_thresholds: List[float],
            bin_size: float,
            simple_index_cols: List[str]
    ):
        """
        Create table `out_table` of TFMRA retracked elevations estimating
        ice surface elevation from the dominant scattering interface
        """

        logger = self.context_logger("TFMRA")

        if self.session.table_exists(out_table):
            self._log_table_exists(logger, out_table)
            return

        # reshape TFMRA elevations into rows
        tfmra_elvtns = calc_tfmra_elevation(
            retracker_thresholds, scaled_waveform, first_bin_elvtn, bin_size,
            logger
        ).T.reshape(-1, 1)

        #
        tfmra_labels = np.tile(
            retracker_thresholds, (scaled_waveform.shape[0], 1)
        ).T.reshape(-1, 1)

        id_labels = np.tile(
            waveform_id, len(retracker_thresholds)
        ).reshape(-1, 1)

        out_data = np.hstack(
            [id_labels, tfmra_labels, tfmra_elvtns]
        )

        logger.info(f"creating retracked elevation table")

        # pandas seems to change the values slightly when converting types?
        # df = pd.DataFrame(
        #     data=np.hstack([id_labels, tfmra_labels, tfmra_elvtns]),
        #     columns=[id_col, threshold_col, elevation_col]
        # )

        logger.info(f"writing to output table {out_table}")

        # slower than direct insertion but doesn't require specifying
        # column types
        # ^ that was a bad decision
        # self.session.create_table_from_dataframe(
        #     out_table, col_config, df
        # )

        col_config_create = column_config_dict_to_list(col_config)
        self.session.create_table(out_table, col_config_create)
        # ignore rows object warning
        self.session.insert(out_table, out_data.tolist())

        self._simple_index_on_cols(logger, out_table, simple_index_cols)
        self.session.commit()

    def create_wshape_table(
            self,
            out_table: Table,
            col_config: ColumnConfigDict,
            scaled_waveform: np.ndarray,
            waveform_id: np.ndarray,
            ppeak_indices_left: Iterable[int],
            ppeak_indices_right: Iterable[int],
            bin_size: float,
            signal_threshold: float,
            simple_index_cols: List[str]
    ):
        """
        Create table `out_table` with pulse peakiness and return width
        calculated from `scaled_waveform`.
        """

        logger = self.context_logger("Waveform Shape")

        if self.session.table_exists(out_table):
            self._log_table_exists(logger, out_table)
            return

        peak_value = np.amax(scaled_waveform, 1)
        peak_index = np.argmax(scaled_waveform, 1)

        # Pulse Peakiness
        logger.info("calculating pulse peakiness")
        # first peak value over sum of all other values
        sum_full = np.sum(scaled_waveform, 1)
        sum_full[sum_full == 0] = np.nan  # prevent divide by zero
        ppeak_full = peak_value / np.sum(scaled_waveform, 1)

        # pulse peakiness of bins left of peak
        sum_left = np.apply_along_axis(
            lambda a: aggregate_relative_indices(
                a, ppeak_indices_left, np.argmax, np.sum
            ),
            1, scaled_waveform
        )
        sum_left[sum_left == 0] = np.nan
        ppeak_left = peak_value / sum_left

        # pulse peakiness of bins right of peak
        sum_right = np.apply_along_axis(
            lambda a: aggregate_relative_indices(
                a, ppeak_indices_right, np.argmax, np.sum
            ),
            1, scaled_waveform
        )
        sum_right[sum_right == 0] = np.nan
        ppeak_right = peak_value / sum_right

        # Return Width
        logger.info("calculating return width")
        # get left and right indices that mark the boundaries of the signal
        # for each scaled waveform
        return_bound_left = np.ndarray.astype(
            np.apply_along_axis(
                lambda a: lin_interp_from_first_max(
                    a, signal_threshold, InterpolateDirection.LEFT
                ),
                1, scaled_waveform
            ),
            dtype=float
        )
        return_bound_right = np.ndarray.astype(
            np.apply_along_axis(
                lambda a: lin_interp_from_first_max(
                    a, signal_threshold, InterpolateDirection.RIGHT
                ),
                1, scaled_waveform
            ),
            dtype=float
        )

        rwidth_left = (peak_index - return_bound_left) * bin_size
        rwidth_right = (peak_index + return_bound_right) * bin_size
        rwidth_full = rwidth_left + rwidth_right

        # df = pd.DataFrame(
        #     np.vstack([
        #         waveform_id,
        #         ppeak_full, ppeak_left, ppeak_right,
        #         rwidth_full, rwidth_left, rwidth_right
        #     ]).T,
        #     columns=[
        #         waveform_id_col,
        #         ppeak_col, ppeak_left_col, ppeak_right_col,
        #         rwidth_col, rwidth_left_col, rwidth_right_col
        #     ]
        # )
        #
        # logger.info(f"writing to output table {out_table}")
        # self.session.create_table_from_dataframe(out_table, df)

        out_data = np.vstack([
            waveform_id,
            ppeak_full, ppeak_left, ppeak_right,
            rwidth_full, rwidth_left, rwidth_right
        ]).T

        col_config_create = column_config_dict_to_list(col_config)
        self.session.create_table(out_table, col_config_create)
        # ignore rows object warning
        self.session.insert(out_table, out_data.tolist())

        self._simple_index_on_cols(logger, out_table, simple_index_cols)

        self.session.commit()

    def create_wscaled_table(
            self,
            out_table: Table,
            col_config: ColumnConfigDict,
            waveform_id: np.ndarray,
            scaled_waveform: np.ndarray,
            simple_index_cols: List[str]
    ):
        """
        Create a table `out_table` with the `scaled_waveform` and their
        associated `waveform_id`.
        """

        logger = self.context_logger("Scaled Waveform")

        if self.session.table_exists(out_table):
            self._log_table_exists(logger, out_table)
            return

        # # zip tolisted so the waveform dimension is combined into a
        # # single column (2 columns total)
        # df = pd.DataFrame(
        #     zip(waveform_id.tolist(), scaled_waveform.tolist()),
        #     columns=[waveform_id_col, scaled_waveform_col]
        # )

        logger.info(f"writing to output table {out_table}")
        # self.session.create_table_from_dataframe(
        #     out_table, df
        # )

        out_data = zip(
            waveform_id.tolist(), list(iter(scaled_waveform.tolist()))
        )

        col_config_create = column_config_dict_to_list(col_config)
        self.session.create_table(out_table, col_config_create)
        # ignore rows object warning
        self.session.insert(out_table, out_data)

        self._simple_index_on_cols(logger, out_table, simple_index_cols)

        self.session.commit()

    def offset_calibration(
            self,
            out_table: Table,
            offset_calib_params: Dict[str, List[str]],
            query_kwargs: KwargsDict,
            simple_index_cols=(COL.offset_calib, COL.tfmra_threshold),
            query=queries.sensor_offset_samples
    ):
        logger = self.context_logger('Offset Calibration Samples')
        if not self.session.table_exists(out_table):

            # format the parameters for each offset calibration
            calib_queries = []
            for name, where_statements in offset_calib_params.items():
                main_kwargs = {**query_kwargs, 'calib_name': name}

                main_query = self.format_query_with_base_args(
                    query, main_kwargs
                )

                where_block = sql_block_where(
                    (pgs.SQL(statement) for statement in where_statements)
                )

                final_query = stack_sql_lines(main_query, where_block)

                calib_queries.append(final_query)

            combined_query = union_sql_blocks(
                *calib_queries
            )

            logger.info(f"creating table {out_table}")
            self.session.create_table_as(out_table, combined_query)

            self._simple_index_on_cols(logger, out_table, simple_index_cols)
            self.session.commit()

        else:
            self._log_table_exists(logger, out_table)


def _process_msg(
        title: str,
        name: str,
        time: str,
        duration: Optional[str] = None,
        msg: Optional[str] = None,
        prefix=">>>>\t"
) -> str:
    return (
            f"\n"
            f"{prefix}{title} PROCESS '{name}'\t\n"
            f"{prefix}TIME: {time}\t\n"
            + (f"{prefix}DURATION: {duration}" if duration else "")
            + (f"{prefix}MESSAGE:\n\n{msg}\n\n" if msg else "")
    )


@contextmanager
def new_process(
        name: str,
        session: Session,
        logger_hub=empty_logger_hub()
) -> ContextManager[Process]:
    logger = logger_hub.context("Process Manager")
    time_start = datetime.now()

    try:
        logger.info(_process_msg(
            "STARTED", name, str(time_start),
        ))
        yield Process(name, session, logger_hub)

    except Exception as e:
        time_end = datetime.now()
        logger.info(_process_msg(
            "FAILED", name, str(time_end)
        ))
        logger.exception(e)
        raise e

    finally:
        time_end = datetime.now()
        logger.info(_process_msg(
            "COMPLETED", name, str(time_end), str(time_end - time_start)
        ))
