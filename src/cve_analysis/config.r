source("tools.r")

# NOTE: redefine any of these at the beginning of a plot_... script to modify them
# for one plot specifically

cfg.dir = dirname(dirname(getwd()))
cfg.name = "config.ini"
cfg.filter = "*.ini"

# Get configuration ini
config = get.config(cfg.dir, cfg.name, cg.filter)

# PostgreSQL database ----
# modify these parameters to point to your postgis database
# these should match config.ini
db.driver = dbDriver("PostgreSQL")
db.name = config$Database$dbname
db.host = config$Database$host
db.port = as.integer(config$Database$port)
db.user = config$Database$user
db.password = config$Database$password
db.schema = config$Database$default_schema # inserted into the query directly

# Query Props ----
# use to filter out certain results when the analysis narrows in scope
# as the manuscript progresses

qp.grid_zone = 3 # grid zone with confirmed flat surface and no deformed ice
qp.fp_size = 14 # most appropriate footprint radius given plot_footprint-error
qp.offset_calib = 'main' # current offset calibration method
qp.pit_dist = 14

# Plot Labels ----
# model parameters
lb.threshold = "TFMRA Threshold (%)"
lb.fp_size = "Aggregation Footprint Radius (m)"

# intermediate
lb.offset = "Sensor Offset (m)"
lb.offset_sd = "Sensor Offset Std. Dev. (m)"

# error
lb.e = "Error (cm)"
lb.e_r = TeX("Error as a Prop. of Snow Depth $E_r$ (%)")
lb.e_a = TeX("Abs. Error $E_a$ (cm)")
lb.p_r = TeX("Penetration as a Prop. of Snow Depth $P_r$ (%) ")
lb.isp = "In Snowpack (%)"

# error stats
lb.e_ra_mean = TeX("Mean Abs. Error as a Prop. of Snow Depth $E_{ra}$ (%)")
lb.e_ra_sd = TeX("Abs. Error as a Prop. of Snow Depth Std. Dev  $E_{ra}$ (%)")
lb.p_r_mean = TeX("Mean Penetration as a Prop. of $P_r$ (%) ")

# surface properties
lb.deform = "Ice Type"
lb.sdepth = "Snow Depth (cm)"
lb.htopo = "H-Topo (cm)"
lb.salin = "Salinity Present"
lb.pp = "Pulse Peak."
lb.ppl = "Pulse Peak. Left"
lb.ppr = "Pulse Peak. Right"
lb.pplr = "Pulse Peak. Ratio"



# SQL Snippets ----
# commonly reused blocks to be inserted into sql queries

# SELECT: all error results scaled to plottable values
sq.error = "
penetration*100 p,
rel_penetration*100 p_r,
error*100 e,
abs_error*100 e_a,
rel_error*100 e_r,
abs_rel_error*100 e_ra,
in_snowpack*100 isp,
above_snow*100 asn,
below_ice*100 bic"

sq.error_stat = "
avg(penetration)*100 p_mean,
avg(rel_penetration)*100 p_r_mean,
avg(error)*100 e_mean,
avg(abs_error)*100 e_a_mean,
avg(rel_error)*100 e_r_mean,
avg(abs_rel_error)*100 e_ra_mean,
stddev(penetration)*100 p_sd,
stddev(rel_penetration)*100 p_r_sd,
stddev(error)*100 e_sd,
stddev(abs_error)*100 e_a_sd,
stddev(rel_error)*100 e_r_sd,
stddev(abs_rel_error)*100 e_ra_sd"

sq.map.ice = sql.query(
  "( VALUES (0, 'Undeformed'), (1, 'Deformed') )"
)

sq.map.salin = sql.query(
  "( VALUES (false, 'No Salinity'), (true, 'Salinity Present') )"
)

# Make functions from tools ----
save.plot = makefunc.save.plot("", config$Analysis$plot_dir)
sql.fetch = makefunc.sql.fetch(
  db.driver, db.name, db.host, db.port, db.user, db.password
)