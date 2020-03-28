"""
Describes ASIRAS measurement datasets binary format

full binary format specification in PDF document:
    REF: CS-LI-ESA-GS-0371
    Issue: 2.6.1
    Name: cryovex airborne data descriptions
    Section: 3.2.4 (page 35)
"""

import re

header_encoding = 'ASCII'
header_regex = re.compile(
    r'([a-z_]+)="?([^\n<>"]+)(?:<[^\n<>]+>)?"?\n',
    re.I
)
mph_bytes = 1247
sph_bytes = 1112
dsh_bytes = 280
cg_bytes = 64
awg_bytes = 556
rows_per_block = 20
dsh_count_key = 'NUM_DSD'
dsh_name_key = 'DS_NAME'
dsh_offset_key = 'DS_OFFSET'
dsh_blocks_key = 'NUM_DSR'
dsh_asiras_prefix = 'ASI'
byte_order = '>'
skip_field = "~"

_type_int = "bigint"
_type_float = "double precision"

# name, read type (struct), write type (psql), scale, count
fields_format = dict(
    # time and orbit group
    tog=[
        ['days', 'l', _type_int, 1, 1],
        ['seconds', 'L', _type_int, 1, 1],
        ['microseconds', 'L', _type_int, 1, 1],
        [skip_field, 'x', None, 1, 8],
        ['instrument_config', 'L', _type_int, 1, 1],
        ['burst_counter', 'L', _type_int, 1, 1],
        ['latitude', 'l', _type_float, 1.e-7, 1],
        ['longitude', 'l', _type_float, 1.e-7, 1],
        ['altitude', 'l', _type_float, 1.e-3, 1],
        ['altitude_rate', 'l', _type_float, 1.e-6, 1],
        ['velocity_xyz', 'l', _type_float, 1.e-3, 3],
        ['beam_direction_xyz', 'l', _type_float, 1.e-6, 3],
        ['interferometer_baseline_xyz', 'l', _type_float, 1.e-6, 3],
        ['confidence_data', 'L', _type_int, 1, 1]
    ],
    # measurements group
    mg=[
        ['window_delay', 'q', _type_float, 1.e-12, 1],
        [skip_field, 'x', None, 1, 4],
        ['ocog_width', 'l', _type_float, 1.e-2, 1],
        ['retracker_range', 'l', _type_float, 1.e-3, 1],
        ['surface_elvtn', 'l', _type_float, 1.e-3, 1],
        ['agc_ch1', 'l', _type_float, 1.e-2, 1],
        ['agc_ch2', 'l', _type_float, 1.e-2, 1],
        ['tfg_ch1', 'l', _type_float, 1.e-2, 1],
        ['tfg_ch2', 'l', _type_float, 1.e-2, 1],
        ['transmit_power', 'l', _type_float, 1.e-6, 1],
        ['doppler_range', 'l', _type_float, 1.e-3, 1],
        ['instr_range_corr_ch1', 'l', _type_float, 1.e-3, 1],
        ['instr_range_corr_ch2', 'l', _type_float, 1.e-3, 1],
        [skip_field, 'x', None, 1, 8],
        ['intern_phase_corr', 'l', _type_float, 1.e-6, 1],
        ['extern_phase_corr', 'l', _type_float, 1.e-6, 1],
        ['noise_power', 'l', _type_float, 1.e-2, 1],
        ['roll', 'h', _type_float, 1.e-3, 1],
        ['pitch', 'h', _type_float, 1.e-3, 1],
        ['yaw', 'h', _type_float, 1.e-3, 1],
        [skip_field, 'x', None, 1, 2],
        ['heading', 'l', _type_float, 1.e-3, 1],
        ['std_roll', 'H', _type_float, 1.e-4, 1],
        ['std_pitch', 'H', _type_float, 1.e-4, 1],
        ['std_yaw', 'H', _type_float, 1.e-4, 1]
    ],
    # multilooked waveform group
    mwg=[
        ['ml_power_echo', 'H', _type_int, 1, 256],
        ['linear_scale_factor', 'l', _type_int, 1, 1],
        ['power2_scale_factor', 'l', _type_int, 1, 1],
        ['num_ml_power_echoes', 'H', _type_int, 1, 1],
        ['flags', 'H', 'integer', 1, 1],
        ['beam_behaviour', 'H', _type_int, 1, 50]
    ]
)
