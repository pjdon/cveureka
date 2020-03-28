"""
Processing queries
"""

setup = \
"""
-- Returns the coefficient of speed reduction relative
-- to c (speed of light) for radar travelling through a snow pack
-- of density `snow_dens_kgpm3` of units kg/m3
-- Adapted from Kurtz et. al. 2013 doi:10.5194/tc-7-1035-2013
CREATE OR REPLACE FUNCTION
    {T@c_snow_dens_coeff}(snow_dens_kgpm3 NUMERIC)
    RETURNS NUMERIC AS $$
    BEGIN
        RETURN 1/sqrt(1+(2*(snow_dens_kgpm3/1000)));
    END;
$$ LANGUAGE plpgsql;

-- Returns the dominant scattering interface elevation `dsi_elvtn`
-- shifted upwards towards the snow surface `snow_surf_elvtn`
-- to account for the reduction in the speed of radar through
-- a snowpack of density `snow_dens_kgpm3` in kg/m3
-- If `dsi_elvtn` is above `snow_surf_elvtn` then `dsi_elvtn`
-- is returned
CREATE OR REPLACE FUNCTION
    {T@adjust_dsi_by_snow_dens}(
        dsi_elvtn NUMERIC,
        snow_surf_elvtn NUMERIC,
        snow_dens_kgpm3 NUMERIC
    ) RETURNS NUMERIC AS $$
    BEGIN
        IF dsi_elvtn >= snow_surf_elvtn THEN
            RETURN dsi_elvtn;
        ELSE
            RETURN snow_surf_elvtn-(
                (snow_surf_elvtn-dsi_elvtn)
                *{T@c_snow_dens_coeff}(snow_dens_kgpm3)
            );
        END IF;
    END;
$$ LANGUAGE plpgsql;

"""

label_grid_zones = \
"""
SELECT {I@src_id}, {I@grid_zone}
FROM {T@src} src
JOIN {T@zones} gzn
    ON st_contains(gzn.{I@geom}, src.{I@geom})
ORDER BY {I@src_id}
"""


summarize_pits = \
"""
WITH
-- required fields from salinity
_saln AS (
SELECT {I@id_pit}, salinity_psu, hag_top, hbs_top,
    pit_snow_depth, section_height
FROM {T@pit_salin}
),
-- any salinity present in the snowpack
_saln_any_inpack AS (
SELECT {I@id_pit},
    MAX(salinity_psu) > 0 saline_snowpack
FROM _saln
WHERE hag_top <> 0
GROUP BY {I@id_pit}
),
-- proportional (using relative thickness) snowpack saltiness
_saln_avg_salinity_psu AS (
SELECT {I@id_pit},
AVG(salinity_psu * section_height/pit_snow_depth) salinity_mean
FROM _saln
WHERE hag_top <> 0
GROUP BY {I@id_pit}
),
-- maximum salinity_psu in any level
_saln_max_salinity_psu AS (
SELECT {I@id_pit},
MAX(salinity_psu) salinity_max
FROM _saln
GROUP BY {I@id_pit}
),
-- salinity_psu at lowest level
_saln_ice_level AS (
SELECT {I@id_pit}, salinity_psu salinity_ice
FROM _saln WHERE hag_top = 0
),
-- all levels which have nonzero salinity_psu
_saln_nonzero AS (
SELECT * FROM _saln WHERE salinity_psu <> 0
),
-- depth in m to top nonzero salinity_psu level
_saln_top_salty_depth AS (
SELECT {I@id_pit}, MIN(hbs_top) top_saline_depth
FROM _saln_nonzero GROUP BY {I@id_pit}
),
-- proportion of snow depth to top nonzero salinity_psu level
_saln_top_salty_prop AS (
SELECT {I@id_pit},
    MIN(hbs_top)/AVG(pit_snow_depth) top_saline_prop
FROM _saln_nonzero GROUP BY {I@id_pit}
),
-- snow depth in m that has salty layers
_saln_salty_meters AS (
SELECT {I@id_pit}, COALESCE(SUM(section_height),0) total_saline_meters
FROM _saln_nonzero
GROUP BY {I@id_pit}
),
-- proportion of snow depth that has salty layers
_saln_salty_prop AS (
SELECT {I@id_pit},
    COALESCE(SUM(section_height)/AVG(pit_snow_depth),0)
        total_saline_prop
FROM _saln_nonzero
GROUP BY {I@id_pit}
),
-- combine all salinity characteristics
_saln_full AS (
SELECT *
FROM  _saln_ice_level
    JOIN _saln_any_inpack USING ({I@id_pit})
    JOIN _saln_avg_salinity_psu USING ({I@id_pit})
    JOIN _saln_max_salinity_psu USING ({I@id_pit})
    JOIN _saln_top_salty_depth USING ({I@id_pit})
    JOIN _saln_top_salty_prop USING ({I@id_pit})
    JOIN _saln_salty_meters USING ({I@id_pit})
    JOIN _saln_salty_prop USING ({I@id_pit})
),
-- required fields from stratigraphy
_strat AS (
SELECT {I@id_pit}, hbs_top, section_height, gs1, gs2, gs3, gl1, gl2, gl3,
    grain_type, has_crust, has_slab, pit_snow_depth
FROM {T@pit_strat}
),
-- aggregate simple fields
_strat_main AS (
SELECT {I@id_pit},
    AVG(((gs1+gs2+gs3+gl1+gl2+gl3)/6)) grain_size_mean,
    AVG(((gs1*gl1)+(gs2*gl2)+(gs3*gl3))/3) grain_area_mean,
    AVG(((gs1/gl1)+(gs2/gl2)+(gs3/gl3))/3) grain_ratio_mean
FROM _strat GROUP BY {I@id_pit}
),
-- count total meters of snow pack for each type of snow
_strat_meters_round AS (
SELECT {I@id_pit}, SUM(section_height) meters_round
FROM _strat WHERE grain_type='Round' GROUP BY {I@id_pit} ),
_strat_meters_facet AS (
SELECT {I@id_pit}, SUM(section_height) meters_facet
FROM _strat WHERE grain_type='Faceted' GROUP BY {I@id_pit} ),
_strat_meters_mixed AS (
SELECT {I@id_pit}, SUM(section_height) meters_mixed
FROM _strat WHERE grain_type='Mixed Snow' GROUP BY {I@id_pit} ),
_strat_meters_hoar AS (
SELECT {I@id_pit}, SUM(section_height) meters_hoar
FROM _strat WHERE grain_type='Depth Hoar' GROUP BY {I@id_pit} ),
-- combine all types
_strat_meters AS (
SELECT {I@id_pit},
    COALESCE(meters_round,0) meters_round,
    COALESCE(meters_facet,0) meters_facet,
    COALESCE(meters_mixed,0) meters_mixed,
    COALESCE(meters_hoar,0) meters_hoar
FROM _strat_meters_round
    FULL OUTER JOIN _strat_meters_facet USING ({I@id_pit})
    FULL OUTER JOIN _strat_meters_mixed USING ({I@id_pit}) 
    FULL OUTER JOIN _strat_meters_hoar USING ({I@id_pit})
),
-- calculate proportions for all types
_strat_prop AS (
SELECT DISTINCT {I@id_pit},
    meters_round/pit_snow_depth prop_round,
    meters_facet/pit_snow_depth prop_faceted,
    meters_mixed/pit_snow_depth prop_mixed,
    meters_hoar/pit_snow_depth prop_hoar
FROM _strat_meters JOIN _strat USING ({I@id_pit})
),
-- combine all stratigraphy characteristics
_strat_full AS (
SELECT * FROM
    _strat_main
    JOIN _strat_meters USING ({I@id_pit})
    JOIN _strat_prop USING ({I@id_pit})
),
-- required fields and row number from density
_dens AS (
SELECT {I@id_pit},
    snow_density,
    section_height,
    pit_snow_depth,
    row_number()
        OVER (PARTITION BY {I@id_pit} ORDER BY {I@id_pit}, hbs_top)
        obsv_num
FROM {T@pit_dens}
WHERE {I@id_pit} IN (
    SELECT {I@id_pit}
    FROM pit_dens
    GROUP BY 1
    HAVING bool_and(snow_density IS NOT NULL)
    ORDER BY {I@id_pit}
)
),
-- aggregate fields from density
_dens_full AS (
SELECT {I@id_pit},
    SUM(snow_density*section_height/pit_snow_depth) dens_mean,
    MIN(snow_density) dens_min,
    MAX(snow_density) dens_max,
    MAX(snow_density)-MIN(snow_density) dens_range
FROM _dens GROUP BY {I@id_pit}
),
-- required fields and row number from temperature
_temp AS (
SELECT {I@id_pit},
    temp_c,
    hbs,
    4 section_height, -- all sections are 4cm
    pit_snow_depth,
    row_number()
        OVER (PARTITION BY {I@id_pit} ORDER BY {I@id_pit}, hbs)
        obsv_num
FROM {T@pit_temp}
WHERE hbs >= 0
ORDER BY {I@id_pit}, hbs
),
-- aggregate fields from temperature
_temp_full AS (
SELECT {I@id_pit},
    AVG(temp_c*section_height/pit_snow_depth) temp_mean,
    MIN(temp_c) temp_min,
    MAX(temp_c) temp_max,
    MAX(temp_c)-MIN(temp_c) temp_range
FROM _temp GROUP BY {I@id_pit}
),
-- combine all characteristics
_chars AS (
SELECT DISTINCT *
FROM _saln_full
    FULL OUTER JOIN _strat_full USING ({I@id_pit})
    FULL OUTER JOIN _temp_full USING ({I@id_pit})
    FULL OUTER JOIN _dens_full USING ({I@id_pit})
    JOIN {T@pit_info} USING ({I@id_pit})
ORDER BY {I@id_pit}
)
SELECT * FROM _chars
"""


# bottom elevation points from depths by subtracting
# depth at points from nearest surface elevation point
elvtn_bottom = \
"""\
SELECT nearest.*
FROM {T@mgn} mgn
JOIN LATERAL (
    SELECT
        mgn.{I@id_mgn},
        als.{I@id_als},
        als.{I@snow_elvtn} - mgn.{I@snow_depth} {I@ice_elvtn},
        st_distance(mgn.{I@geom}, als.{I@geom}) point_dist,
        mgn.{I@geom} {I@geom}
    FROM {T@als} als
    ORDER BY mgn.{I@geom} <-> als.{I@geom}
    LIMIT 1
) as nearest USING ({I@id_mgn})
ORDER BY {I@id_mgn}
"""

# ASIRAS points filtered to those with acceptable pitch, roll and within
# distance of magnaprobe data (since ASIRAS is always near ALS data)
refine_asr = \
"""\
WITH
_filtered AS (
SELECT {I@id_asr}, {I@geom}
FROM {T@asr}
WHERE roll BETWEEN -{L@roll_dev} AND {L@roll_dev}
AND pitch BETWEEN -{L@pitch_dev} AND {L@pitch_dev}
)
SELECT DISTINCT f.*
FROM _filtered f
    INNER JOIN {T@obs} p
        ON st_dwithin(f.{I@geom}, p.{I@geom}, {L@max_fp_radius})
GROUP BY f.{I@id_asr}, f.{I@geom}
HAVING COUNT(*) >= {L@near_points}
ORDER BY {I@id_asr}
"""

# buffer points by distance and merge buffers to single polygon
buffer_zone = \
"""\
SELECT st_buffer({I@geom}, {L@dist}) {I@geom}
FROM {T@points}
"""

# select A where its geometry intersects with B
select_intersect = \
"""\
SELECT DISTINCT a.*
FROM {T@a} a JOIN {T@b} b
ON st_intersects(a.{I@geom}, b.{I@geom})
"""

asr_footprints = \
"""\
WITH
_vars AS (
SELECT
    {I@id_asr} AS id,
    -- range to ground
    -- use altitude if unreasonable value since ALS shows ~6m elevation
    CASE
        WHEN {I@retracker_range} > 100
        THEN LEAST({I@retracker_range}, {I@altitude})
        ELSE {I@altitude}
        END
    AS rng,
    -- ground speed from DGPS
    SQRT(
        POW({I@velocity_xyz}[1],2)
        +POW({I@velocity_xyz}[2],2)
    ) AS gspeed,
    {I@geom} AS nadir,
    -- direction of travel using next point
    -- use previous direction for last point
    COALESCE(
        st_azimuth({I@geom}, LEAD({I@geom}) OVER w),
        st_azimuth(LAG({I@geom}) OVER w, {I@geom})
    ) AS azimuth
FROM {T@asr}
WINDOW w AS (ORDER BY {I@id_asr})
),
_sizes AS (
SELECT
id, nadir, azimuth,
-- across-track pulse-limited
-- radius of a circle
-- r = sqrt(rng * c * plength)
sqrt(rng * 3E8 * {L@plength}) AS r,
-- along-track doppler-pulse-limited
-- half-width of a rectangle
-- x = rng * wlength* prf / (2 * n_avg * gspeed)
rng * {L@wlength} * {L@prf} / (2 * {L@n_avg} * gspeed) / 2 AS x
FROM _vars
),
_fp_radar AS (
SELECT id {I@id_asr}, -1 {I@fp_size},
st_intersection(
    st_buffer(nadir, r),
    st_rotate(
        st_makeenvelope(
            st_x(nadir)-r-2,
            st_y(nadir)-x,
            st_x(nadir)+r+2,
            st_y(nadir)+x,
            st_srid(nadir)
        ),
        -azimuth,
        nadir
    )
) AS {I@geom}
FROM _sizes
),
_fp_buffer AS (
SELECT {I@id_asr}, {I@fp_size}, st_buffer({I@geom}, {I@fp_size}) {I@geom}
FROM {T@asr_refined},  {fp_radii_values} s({I@fp_size})
)
(SELECT * FROM _fp_radar)
UNION
(SELECT * FROM _fp_buffer)
ORDER BY {I@id_asr}, {I@fp_size}
"""

aggregate_observations = \
"""\
SELECT
    f.{I@id_asr} {I@id_asr},
    f.{I@fp_size} {I@fp_size},
    MIN(p.{I@val}) {I@val_min},
    MAX(p.{I@val}) {I@val_max},    
    AVG(p.{I@val}) {I@val_mean},
    COUNT(*) {I@val_count},
    (   PERCENTILE_CONT({L@rough_margin}) WITHIN GROUP
            (ORDER BY p.{I@val} DESC) -
        PERCENTILE_CONT(1-{L@rough_margin}) WITHIN GROUP
            (ORDER BY p.{I@val} DESC)
    ) {I@val_rough}
FROM {T@obsv} p
JOIN {T@ftpr} f ON st_contains(f.{I@geom}, p.{I@geom})
GROUP BY f.{I@id_asr}, f.{I@fp_size}
ORDER BY f.{I@id_asr}, f.{I@fp_size}
"""

interpolate_snow_density = \
"""\
WITH
_near_2 AS (
SELECT
    asr.{I@id_asr},
    _near.snow_dens,
    st_distance(asr.{I@geom}, _near.{I@geom}) dist
FROM {T@asr} asr
    JOIN LATERAL (
        SELECT asr.{I@id_asr}, ({I@snow_rho_1} + {I@snow_rho_2})/2 snow_dens,
            dens.{I@geom}
        FROM {T@dens} dens
        ORDER BY asr.{I@geom} <-> dens.{I@geom} LIMIT 2
    ) AS _near USING ({I@id_asr})
ORDER BY asr.{I@id_asr}
),
-- sum the distances for interpolation in next step
_dist_sums AS (
SELECT {I@id_asr}, SUM(dist)  dist_sum
FROM _near_2 GROUP BY {I@id_asr}
ORDER BY {I@id_asr}
),
-- calc the avg density among 2 nearest and
-- also interpolate between them by relative distance
_scaled AS (
SELECT {I@id_asr}, snow_dens,
    snow_dens*(dist/dist_sum) snow_dens_scaled,
    dist
FROM _near_2 JOIN _dist_sums USING ({I@id_asr})
),
_combined_pairs AS (
SELECT {I@id_asr},
    SUM(snow_dens_scaled) {I@snow_dens_interp},
    MIN(dist) {I@dist_near},
    MAX(dist) {I@dist_far}
FROM _scaled
GROUP BY {I@id_asr} ORDER BY {I@id_asr}
)
SELECT * FROM _combined_pairs
"""

sensor_offset_samples = \
"""\
SELECT
    {I@id_asr},
    {L@calib_name} {I@offset_calib},
    {I@tfmra_threshold},
    ({I@snow_elvtn_mean}-{I@snow_depth_mean}) -- observed ice elvtn
    - {I@tfmra_elvtn} -- estimated ice elvtn
    -- adjusts retracked elevation for radar penetration
    -- adapted from Kurtz et. al. 2013 doi:10.5194/tc-7-1035-2013
    + {I@snow_depth_mean}
    * (1-{T@c_snow_dens_coeff}({I@snow_dens_interp}::numeric))
    AS {I@sensor_offset}
FROM {T@tfmra}
JOIN {T@src} USING ({I@id_asr})
JOIN {T@aggr} USING ({I@id_asr})
JOIN {T@snow_dens} USING ({I@id_asr})
JOIN {T@grid_zone} USING ({I@id_asr})\
"""

retracker_error = \
"""
WITH
_offset AS (
SELECT offset_calib, tfmra_threshold, AVG(sensor_offset) sensor_offset
FROM c20.offset_samples
GROUP BY offset_calib, tfmra_threshold
),
_elv AS (
SELECT id_asr, fp_size, offset_calib, tfmra_threshold, sensor_offset, dens_adj,
    tfmra_elvtn,
    tfmra_elvtn+sensor_offset offset_elvtn,
    snow_depth_mean snow_depth,
    snow_elvtn_mean snow_elvtn,
    snow_elvtn_mean-snow_depth_mean ice_elvtn,
    snow_dens_interp snow_dens,
    CASE
        WHEN dens_adj
        THEN 
            adjust_dsi_by_snow_dens(
                (tfmra_elvtn+sensor_offset)::numeric,
                snow_elvtn_mean::numeric,
                snow_dens_interp::numeric
            )
        ELSE
            tfmra_elvtn+sensor_offset
     END adjust_elvtn
FROM c20.asr_tfmra
JOIN _offset USING (tfmra_threshold)
JOIN c20.asr_aggr USING (id_asr)
JOIN c20.asr_snow_dens USING (id_asr),
(VALUES (TRUE),(FALSE)) adj(dens_adj)
WHERE snow_depth_count >=1 and snow_elvtn_count >= 1
),
_error AS (
SELECT id_asr, fp_size, offset_calib, tfmra_threshold, adjust_elvtn, dens_adj,
    snow_elvtn-adjust_elvtn penetration,
    (snow_elvtn-adjust_elvtn)/snow_depth rel_penetration,
    adjust_elvtn-ice_elvtn error,
    (adjust_elvtn-ice_elvtn)/snow_depth rel_error,
    @(adjust_elvtn-ice_elvtn) abs_error,
    @(adjust_elvtn-ice_elvtn)/snow_depth abs_rel_error,
    (adjust_elvtn>snow_elvtn)::integer above_snow,
    (adjust_elvtn<ice_elvtn)::integer below_ice,
    (adjust_elvtn<=snow_elvtn AND adjust_elvtn>=ice_elvtn)::integer in_snowpack
FROM _elv
)
SELECT * FROM _error
"""