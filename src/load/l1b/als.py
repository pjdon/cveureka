"""
Reads ALS L1B format and streams to PostGIS database

full binary format specification in PDF document:
    REF: CS-LI-ESA-GS-0371
    Issue: 2.6.1
    Name: cryovex airborne data descriptions
    Section: 3.2.12 (page 72)
"""

from struct import unpack, calcsize
from math import isnan

from ...logger import empty_logger
from ...postgis.session import Table, Session
from ...postgis.template_query import TemplateQuery

insert_template = \
    "(%s,ST_Transform(ST_SetSRID(" \
    "ST_MakePoint(%s, %s), {L@srid_input}),{L@srid_output}))"


class AlsLoader:

    def __init__(self, logger=empty_logger()):
        self.logger = logger

    def extract_to_database(
            self,
            session: Session,
            file_path: str,
            output_table: Table,
            col_oid: str, col_elvtn: str, col_geom: str,
            srid_output: int,
            srid_input=4326,
            lines_to_buffer=5000,
            format_oid="bigserial PRIMARY KEY",
            format_elvtn="double precision",
            header_format="BLB",  # all parameters: "BLBHQHBBLL8B"
            field_type="d",
            theader_line_bytes=4,
            byte_order=">"
    ):
        self.logger.info(
            f"opening source file at {file_path}"
        )

        with open(file_path, 'rb') as file:
            # CREATE TABLE ----
            # hold file while creating table
            self.logger.info(
                f"creating table {output_table}"
            )
            column_config = [
                (col_oid, format_oid),
                (col_elvtn, format_elvtn)
            ]
            session.create_table(output_table, column_config)
            # add geometry column
            # geometry column streamed to avoid extra lat,lon fields due to
            # large number of rows
            # geometry type and dimensions would require external configuration
            session.add_geom_col(output_table, srid_output, "POINT", 2, col_geom)

            # format insertion template with correct spatial reference IDs
            target_cols = [col_elvtn, col_geom]
            template = TemplateQuery(insert_template).format(
                srid_input=srid_input, srid_output=srid_output
            )

            # READ ALS FILE HEADER ----
            header = unpack(
                byte_order + header_format,
                file.read(calcsize(byte_order + header_format))
            )

            # read relevant parameters from file header
            header_bytes, num_lines, points_per_line = header

            # calculate space for timestamp header array that appears
            # before main data
            # it shows a timestamp for each line

            theader_bytes = theader_line_bytes * num_lines

            # data grouped by field in arrays of size n = point_per_line
            # one block:
            # array of n timestamp seconds (4 * n bytes)
            # array of n microseconds (4 * n bytes)
            # array of n latitudes (4 * n bytes)
            # array of n longitudes (4 * n bytes)
            # array of n elevations (4 * n bytes)
            #
            # there are `num_lines` blocks in total

            field_format = f"{byte_order}{points_per_line}{field_type}"
            field_bytes = calcsize(field_format)

            # jump to end of header/start of data
            file.seek(header_bytes + theader_bytes)

            # STREAM FILE TO OUTPUT TABLE ----
            self.logger.info("streaming file to output table")

            buffer = []
            lines_buffered = 0
            lines_written = 0
            points_buffered = 0
            points_written = 0
            expected_points = num_lines * points_per_line


            for line_num in range(num_lines):
                # read a single line
                # each variable `points_per_line` times

                # skip time variable
                # line_time = unpack(field_format, file.read(field_bytes))
                file.seek(field_bytes, 1)

                line_lat = unpack(field_format, file.read(field_bytes))
                line_lon = unpack(field_format, file.read(field_bytes))
                line_elv = unpack(field_format, file.read(field_bytes))

                # package variable lines into per-point tuples
                for row in zip(line_elv, line_lon, line_lat):
                    # filter out rows with any NaN values
                    if not any(map(isnan, row)):
                        buffer.append(row)
                        points_buffered += 1

                lines_buffered += 1

                if lines_buffered >= lines_to_buffer \
                        or line_num >= num_lines - 1:
                    # once the buffer is full or reached end of the file
                    session.insert(output_table, buffer, target_cols, template)

                    buffer = []
                    points_written += points_buffered
                    points_buffered = 0
                    lines_written += lines_buffered
                    lines_buffered = 0

                    self.logger.info(
                        f"{lines_written}/{num_lines} lines written"
                    )

            self.logger.info(
                f"finished streaming\n"
                f"\tlines written: {lines_written}/{num_lines}\n"
                f"\tpoints written: {points_written}/{expected_points}\n"
                f"\t{expected_points - points_written} filtered out due to"
                "NaN values in a field"
            )
