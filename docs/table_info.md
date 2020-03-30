# Result Tables Information

Descriptions for methods output tables and their columns.

## asr_src

ASIRAS source data extracted from L1B. Column descriptions copied from the CryoVEX airborne data description page 47, section 3.2.5.1

Column | Description | Unit
--- | --- | ---
id_asr | Unique ID for each ASIRAS observation |
days | Days since Jan 1st, 2000 |
seconds | Seconds of the day |
microseconds | Microseconds of the second |
instrument_config | Instrument configuration BLOB. See Table 3-22            |
burst_counter | Bust Counter |
latitude | Geodetic latitude of ASIRAS center of baseline |deg
longitude | Longitude of ASIRAS center of baseline |deg
altitude | WGS-84 Ellipsoidal altitude of ASIRAS center of baseline |m
altitude_rate | Altitude rate determined from DGPS |m/s
velocity_xyz | Velocity [x,y,z] from DGPS locations |m/s
beam_direction_xyz | Real antenna beam direction vector [x,y,z] |m
interferometer_baseline_xyz | Interferometer baseline [x,y,z] |m
confidence_data | Measurement Confidence Data. See Table 3-23 |
window_delay | Window Delay |s
ocog_width | OCOG Retracker Width |bins
retracker_range | OCOG derived range |m
surface_elvtn | Surface elevation estimated using OCOG |m
agc_ch1 | AGC Channel 1 |dB
agc_ch2 | AGC Channel 2 |dB
tfg_ch1 | Total fixed gain Channel 1 |dB
tfg_ch2 | Total fixed gain Channel 2 |dB
transmit_power | Transmit Power |W
doppler_range | Doppler range correction |m
instr_range_corr_ch1 | Instrument range correction Channel 1 |m
instr_range_corr_ch2 | Instrument range correction Channel 2 |m
intern_phase_corr | Internal phase correction |rad
extern_phase_corr | External phase correction |rad
noise_power | Noise power |dB
roll | Roll |deg
pitch | Pitch |deg
yaw | Yaw |deg
heading | Heading with regards to local north |deg
std_roll | standard deviation of roll during stack integration |deg
std_pitch | standard deviation of pitch during stack integration |deg
std_yaw | standard deviation of yaw during stack integration |deg
ml_power_echo | Multi-looked power echo |
linear_scale_factor | Linear scale factor |
power2_scale_factor | Power 2 scale factor |
num_ml_power_echoes | number of multi-looked echoes |
flags | Instrument flags BLOB. See Table 3-24 |
beam_behaviour | Beam behavior parameters BLOB. See Table 3-25 |
geom | Geometry column |

## asr_snow_dens

Snow density calculated for each ASIRAS point based on nearest snow density observation and distance-weighted density of two nearest observations from ESC-30 measurements.

Column | Description | Unit
--- | --- | ---
id_asr | Unique ID for each ASIRAS observation |
snow_dens_interp | Distance-weighted snow density of nearest two observations to ASIRAS |kg/m<sup>3</sup>
dist_near | Distance to the nearest snow density observation |m 
dist_far | Distance to the second nearest snow density observation | m

## asr_aggr

Surface observations aggregated to ASIRAS footprints. Aggregations (min, max, mean, etc.) are for observations within each ASIRAS footprint.

Column | Description | Unit
--- | --- | ---
id_asr | Unique ID for each ASIRAS observation |
fp_size | Footprint size code. Radius (m) for circular footprints and -1 for pulse-doppler limited radar footprint |
snow_depth_min | Minimum snow depth |m
snow_depth_max | Maximum snow depth |m
snow_depth_mean | Mean snow depth |m
snow_depth_stddev | Standard deviation of snow depth |m
snow_depth_count | Number of snow depth observations |
snow_depth_rough | Roughness of snow depth |m
snow_elvtn_min | Minimum snow surface elevation (in ASIRAS footprint) |m
snow_elvtn_max | Maximum snow surface elevation |m
snow_elvtn_mean | Mean snow surface elevation |m
snow_elvtn_stddev | Standard deviation of snow surface elevation |m
snow_elvtn_count | Number of snow surface elevation observations |
snow_elvtn_rough | Roughness of snow surface elevation |m
ice_deform_min | Minimum ice deformity (in ASIRAS footprint) |
ice_deform_max | Maximum ice deformity |
ice_deform_mean | Mean ice deformity |
ice_deform_stddev | Standard deviation of ice deformity |
ice_deform_count | Number of ice deformity observations |
ice_deform_rough | Roughness of ice deformity |m
ice_elvtn_min | Minimum ice surface elevation (in ASIRAS footprint) |m
ice_elvtn_max | Maximum ice surface elevation |m
ice_elvtn_mean | Mean ice surface elevation |m
ice_elvtn_stddev | Standard deviation of ice surface elevation |m
ice_elvtn_count | Number of ice surface elevation observations |
ice_elvtn_rough | Roughness of ice surface elevation |m

## asr_tfmra

Ice surface elevation estimated using the Threshold First-Maxima Retracker (TFMRA)  applied to ASIRAS waveforms.

Column | Description | Unit
--- | --- | ---
id_asr | Unique ID for each ASIRAS observation |
tfmra_threshold | Threshold as a fraction of the first peak value used to retrack the ice surface elevation |0 to 1
tfmra_elvtn | Estimate of ice surface elevation | m

## asr_wshape

Shape of ASIRAS radar return waveform characterized using pulse peakiness and return width.

Column | Description | Unit
--- | --- | ---
id_asr | Unique ID for each ASIRAS observation |
ppeak | Pulse peakiness of the first peak |
ppeak_left | Pulse peakiness of three bins just left of the first peak |
ppeak_right | Pulse peakiness of three bins just right of the first peak |
rwidth | Width of the return window which starts at last point on the waveform which is 1% and left of the first peak, and ends at the first point on the waveform which is 1% and right of the first peak. |
rwidth_left | Width from the first peak to the left of the return window. |m
rwidth_right | Width from the first peak to the right of the return window. |m

## asr_wscaled

ASIRAS waveforms scaled to remove the effects of gains and attenuations. See Equation 3.2-3 of the CryoVEX airborne data description (page 51).

Column | Description | Unit
--- | --- | ---
id_asr | Unique ID for each ASIRAS observation |
waveform_scaled | Waveform scaled using Equation 3.2-3 by applying the Linear and Power 2 factors |

## asr_error

Errors of the ASIRAS TFMRA retracked elevation calculated by comparing against the observed ice surface elevation using Magnaprobe snow depth subtracted from ALS snow surface elevation

Column | Description | Unit
--- | --- | ---
id_asr | Unique ID for each ASIRAS observation |
fp_size | Footprint size code. Radius (m) for circular footprints and -1 for pulse-doppler limited radar footprint |
offset_calib | Offset calibration method ('main' for the conditions used in the manuscript, 'ssnow' for the conditions used in the MSc thesis)
tfmra_threshold | Threshold as a fraction of the first peak value used to retrack the ice surface elevation |0 to 1
dens_adj | Whether an adjustment is applied to the TFMRA elevation based on the snow depth and snow density. |True/False
retrack_elvtn | TFMRA retracked ice surface elevation estimate |m
penetration | Penetration of the retracked elevation through the snowpacksnowpack |m
rel_penetration | Penetration relative to the snow depth |
error | Height of the retracked elevation above the observed ice surface elevation |m
abs_error | Absolute value of error |m
rel_error | Error relative to the snow depth |
abs_rel_error | Absolute error relative to the snow depth |
above_snow | Whether the retracked elevation is above the observed snow surface elevation |True/False
below_ice | Whether the retracked elevation is below the observed ice surface elevation |True/False
in_snowpack | Whether the retracked elevation is inside the observed snowpack boundary |True/False

## pit_summary

Summarized snow pit observations.

Column | Description | Unit
--- | --- | ---
id_pit | Unique ID for each snow pit |
salinity_ice | Salinity of the ice layer |PSU
saline_snowpack | Any salinity in snowpack | True/False
salinity_mean | Thickness-weighted snowpack mean salinity | PSU
salinity_max | Maximum salinity | PSU
top_saline_depth | Depth to top saline layer |m
top_saline_prop | Snowpack depth proportion to the the top saline layer |0 to 1
total_saline_meters | Total snowpack saline meters |m
total_saline_prop | Total snowpack saline proportion |0 to 1
grain_size_mean | Mean grain size |mm
grain_area_mean | Mean grain area |mm<sup>2</sup>
grain_ratio_mean | Mean grain height/width ratio |
meters_round | Meters of round grain snowpack layers |m
meters_facet | Meters of facet grain snowpack layers |m
meters_mixed | Meters of mixed grain snowpack layers |m
meters_hoar | Meters of depth hoar grain snowpack layers |m
prop_round | Proportion of round grain snowpack layers |m
prop_facet | Proportion of facet grain snowpack layers |m
prop_mixed | Proportion of mixed grain snowpack layers |m
prop_hoar | Proportion of depth hoar grain snowpack layers |m
temp_mean | Mean snowpack temperature |°C
temp_min | Minimum snowpack temperature |°C
temp_max | Maximum snowpack temperature |°C
temp_range | Snowpack temperature range |°C
dens_mean | Thickness-weighted snowpack mean density |g/cm<sup>3</sup>
dens_min | Minimum snowpack density |g/cm<sup>3</sup>
dens_max | Maximum snowpack density |g/cm<sup>3</sup>
dens_range | Snowpack density range |g/cm<sup>3</sup>
latitude | WGS-84 latitude |deg
longitude | WGS-84 longitude |deg
geom | Geometry column |