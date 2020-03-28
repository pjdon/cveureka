from typing import Tuple, Dict, List, Union
from struct import unpack, calcsize
from itertools import chain

from ...logger import empty_logger
from ...postgis.session import Session
from ...postgis.xtypes import Table

from . import asiras_config as default_config

FieldsFormat = Dict[str, List[List]]


class AsirasLoader:

    def __init__(
            self,
            logger=empty_logger(),
            config=default_config
    ):
        self.logger = logger
        self.config = config

    def _build_column_config(self) -> List[Tuple[str, str]]:
        """
        Return the column configuration as a list of tuples, with each tuple
        representing the column (`name`, `sql type`).

        Data groups (tog,mg,mwg) are flattened since they are not represented
        in the output sql table
        """

        fields_format = self.config.fields_format
        skip_field = self.config.skip_field

        # readability over brevity (e.g. using chain)
        col_config = []

        for group in fields_format.values():
            for row in group:
                col_name = row[0]
                if col_name != skip_field:
                    col_type = row[2]

                    # add a square bracketed SQL count to the column type
                    # if it comes as an array (value count more than 1)
                    values_count = row[4]
                    if values_count > 1:
                        col_type += f"[{values_count}]"

                    col_config.append((col_name, col_type))

        return col_config

    # Excluded since negligible performance boost reading full block at once
    # def _build_struct_formats(self) -> Dict[str, str]:
    #     """
    #     Return the struct string used to read the binary data for each
    #     measurement group.
    #     """
    #
    #     fields_format = self.config.fields_format
    #     byte_order = self.config.byte_order
    #
    #     return {
    #         group: byte_order + "".join([
    #             row[1] + (str(row[4]) if row[4] > 1 else "")
    #             for row in rows
    #         ])
    #         for group, rows in fields_format.items()
    #     }

    def _parse_product_header(self, string: str) -> Dict[str, str]:
        """
        Retrieve `dict` of parameters from product header string using regex
        """
        header_regex = self.config.header_regex

        return {
            match[1]: match[2].strip()
            for match in header_regex.finditer(string)
        }

    def _get_group_read_rules(self, byte_order) \
            -> List[List[Tuple[int, bool, str, bool, Union[int, float]]]]:
        """
        Return instructions for reading a block group of variables
        """

        fields_format = self.config.fields_format
        skip_field = self.config.skip_field

        groups = []

        for group in fields_format.values():
            rules = []
            for name, read_type, write_type, scale, count in group:
                read_format = byte_order + (
                    str(count) + read_type if count > 1 else read_type)
                save_output = name != skip_field
                size = calcsize(read_format)
                multi = count > 1
                rules.append((size, save_output, read_format, multi, scale))
            groups.append(rules)

        return groups

    def _read_block_group(self, file, rules) -> List:
        """
        Read a single group of `rows_per_block` rows
        Convert single item lists to values and apply scaling if necessary.
        """
        buffer = []
        for i in range(self.config.rows_per_block):
            row = []
            for size, save_output, read_format, multi, scale in rules:
                if save_output:
                    value = unpack(read_format, file.read(size))
                    if multi:
                        if scale != 1:
                            row.append([v * scale for v in value])
                        else:
                            # values have to be a list for psycopg2
                            # to recognize them as an array
                            row.append(list(value))
                    else:
                        if scale != 1:
                            row.append(value[0] * scale)
                        else:
                            row.append(value[0])
                else:
                    file.read(size)
            buffer.append(row)
        return buffer

    def extract_to_database(
            self,
            session: Session,
            file_path: str,
            output_table: Table,
            col_id_name: str,
            col_id_format="bigserial PRIMARY KEY",
            blocks_to_buffer=100,
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

            column_config = [(col_id_name, col_id_format)] + \
                            self._build_column_config()
            session.create_table(output_table, column_config)

            # column names for writing to sql table
            target_cols = [name for name, fmt in column_config[1:]]

            # READ PRODUCT HEADERS ----
            self.logger.info(
                f"reader product headers"
            )
            header_encoding = self.config.header_encoding
            # read main product header
            mph = self._parse_product_header(
                file.read(self.config.mph_bytes).decode(header_encoding)
            )
            # read specific product header
            # sph = self._parse_product_header(
            #     file.read(self.config.sph_bytes).decode(header_encoding)
            # )
            # skip since it is not needed
            file.seek(self.config.sph_bytes, 1)

            # number of datasets in dataset header
            dsh_count = int(mph[self.config.dsh_count_key])
            # read data set header
            dsh = [
                self._parse_product_header(
                    file.read(self.config.dsh_bytes).decode(header_encoding)
                )
                for _ in range(dsh_count)
            ]

            # find first dataset header where the name attribute starts with
            # the expected ASIRAS name
            name_key = self.config.dsh_name_key
            name_prefix = self.config.dsh_asiras_prefix
            try:
                asiras_dsh = next(
                    (d for d in dsh
                     if d[name_key].startswith(name_prefix)),
                    None
                )
            except StopIteration:
                raise ValueError(
                    f"No dataset header where attribute f{name_key} has"
                    f"prefix {name_prefix}"
                )

            # parameters from header
            num_blocks = int(asiras_dsh[self.config.dsh_blocks_key])
            start_bytes = int(asiras_dsh[self.config.dsh_offset_key])

            # jump to end of header/start of file
            file.seek(start_bytes)

            # STREAM FILE TO OUTPUT TABLE ----
            self.logger.info("streaming file to output table")

            # build rules for reading and unpacking each data group
            tog_rules, mg_rules, mwg_rules = \
                self._get_group_read_rules(byte_order)

            # bytes needed to skip corrections and average waveform groups
            empty_group_bytes = self.config.cg_bytes + self.config.awg_bytes

            buffer = []
            blocks_buffered = 0
            blocks_written = 0
            rows_buffered = 0
            rows_written = 0

            for block_num in range(1, num_blocks + 1):
                # read a single block
                # `rows_per_block` rows per block

                # time-orbit and measurement groups
                tog = self._read_block_group(file, tog_rules)
                mg = self._read_block_group(file, mg_rules)

                # skip empty groups
                file.seek(empty_group_bytes, 1)

                # multilooked waveform group
                mwg = self._read_block_group(file, mwg_rules)

                # interleave rows of each group and save to buffer
                buffer.extend([
                    tuple(chain(*rows)) for rows in zip(tog, mg, mwg)
                ])

                blocks_buffered += 1
                rows_buffered += self.config.rows_per_block
                expected_rows = num_blocks * self.config.rows_per_block

                # insert buffer into output table, flush if buffer filled
                # or reached end
                if blocks_buffered >= blocks_to_buffer \
                        or block_num >= num_blocks:
                    session.insert(output_table, buffer, target_cols)

                    buffer = []
                    blocks_written += blocks_buffered
                    blocks_buffered = 0
                    rows_written += rows_buffered
                    rows_buffered = 0

                    self.logger.info(
                        f"{blocks_written}/{num_blocks} blocks written"
                    )

            self.logger.info(
                f"finished streaming\n"
                f"\tblocks written: {blocks_written}/{num_blocks}\n"
                f"\trows written: {rows_written}/{expected_rows}\n"
            )
