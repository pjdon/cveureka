# Error vs. threshold grouped by categories of surface properties

source("config.r")

d = sql.fetch(sql.query("
WITH
_map_ice AS (SELECT * FROM {sq.map.ice} s(ice_deform_min, deform))
SELECT (tfmra_threshold*100) threshold,
  snow_depth_mean*100 sdepth,
  deform,
  snow_elvtn_rough*100 htopo,
  ppeak pp,
  ppeak_left ppl,
  ppeak_right ppr,
  ppeak_left/ppeak_right pplr,
  rwidth*100 rw,
  rwidth_left*100 rwl,
  rwidth_right*100 rwr,
  rwidth_left/rwidth_right rwlr,
  abs_error*100 e_a,
  rel_penetration*100 p_r,
  in_snowpack*100 isp
FROM {db.schema}.asr_error
JOIN {db.schema}.asr_aggr USING (id_asr, fp_size)
JOIN {db.schema}.asr_wshape USING (id_asr)
JOIN _map_ice USING (ice_deform_min)
WHERE fp_size = {qp.fp_size}
AND offset_calib = '{qp.offset_calib}'
"))

d.pit = sql.fetch(sql.query("
WITH
_link AS (
SELECT id_pit, id_asr
FROM {db.schema}.pit_info i
JOIN asr_src a ON ST_DWITHIN(a.geom, i.geom, {qp.pit_dist})
),
_map_salin AS (SELECT * FROM {sq.map.salin} s(saline_snowpack, salin))
SELECT (tfmra_threshold*100) threshold,
  salin,
  grain_size_mean gsize,
  abs_error*100 e_a,
  rel_penetration*100 p_r,
  in_snowpack*100 isp
FROM {db.schema}.asr_error
FULL OUTER JOIN _link USING (id_asr)
FULL OUTER JOIN {db.schema}.pit_summary USING (id_pit)
JOIN _map_salin USING (saline_snowpack)
WHERE fp_size = {qp.fp_size}
AND offset_calib = '{qp.offset_calib}'
"))


d$sdepth = pretty.cut(d$sdepth, 3)
d$htopo = pretty.cut(d$htopo, 3)
d$pp = pretty.cut(d$pp, 3)
d$rw = pretty.cut(d$rw, 3)

# PP left/right/ratio have the same relationship as regular PP so probably not worth
# mentioning them
# d$ppl = pretty.cut(d$ppl, 3)
# d$ppr = pretty.cut(d$ppr, 3)
# d$pplr = pretty.cut(d$pplr, 3)


d.pit$gsize = pretty.cut(d.pit$gsize, 3)

# change this if error variables change
msr.vars = c("e_a", "p_r", "isp")
cat.var = "threshold"

aggr.by <- function(target, dfr=d, measures=msr.vars, category=cat.var, fun=mean) {
  return(
    aggregate(dfr[measures], dfr[c(target, category)], fun)
  )
}

# require individual aggregations to preserve categories
ag.sdepth = aggr.by("sdepth")
ag.deform = aggr.by("deform")
ag.htopo = aggr.by("htopo")
ag.salin = aggr.by("salin", d.pit)
ag.gsize = aggr.by("gsize", d.pit)
ag.pp = aggr.by("pp")
# ag.ppl = aggr.by("ppl")
# ag.ppr = aggr.by("ppr")
# ag.pplr = aggr.by("pplr")
ag.rw = aggr.by("rw")



lb.salin = "Snowpack Salinity"
lb.gsize = "Grain\nSize (mm)"
lb.sdepth = "Snow\nDepth (cm)"
lb.htopo = "H-Topo\n(cm)"
lb.pp = "Pulse.\nPeak"
lb.rw = "Return\nWidth (cm)"

# Single Plot Function
var_plot <- function(d, y, c, ylab, clab, extra1=NULL, extra2=NULL) {
  return (
    ggplot(data=d, aes(x=threshold, y=!! enquo(y), color=!! enquo(c)))
    + geom_point()
    + geom_smooth(method=lm, formula=y~poly(x, 3, raw=TRUE), se=FALSE)
    + labs(x=NULL, y=NULL, color=clab)
    + guides(color=guide_legend(nrow=2,byrow=TRUE, title.position = "left"))
    + theme(
      legend.position="top",
      legend.margin=margin(t = 0, unit='cm')
    )
  )
}

# clearer to only show relative penetration
annotate_figure(
  ggarrange(
    var_plot(ag.deform, p_r, deform, lb.p_r, lb.deform),
    var_plot(ag.sdepth, p_r, sdepth, lb.p_r, lb.sdepth),
    var_plot(ag.htopo, p_r, htopo, lb.p_r, lb.htopo),
    var_plot(ag.pp, p_r, pp, lb.p_r, lb.pp),
    var_plot(ag.salin, p_r, salin, lb.p_r, lb.salin),
    var_plot(ag.gsize, p_r, gsize, lb.p_r, lb.gsize),
    nrow=2,
    ncol=3,
    labels = "AUTO"
  ),
  bottom=lb.threshold,
  left=text_grob(lb.p_r, rot = 90)
)
save.plot("p_r_thresh_surf", 1.4, 1.3)
