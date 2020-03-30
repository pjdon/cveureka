# Error distribution vs. threshold

source("config.r")
require(reshape2)

d = sql.fetch(sql.query("
SELECT (tfmra_threshold*100) thresh,{sq.error}
FROM {db.schema}.asr_error
WHERE fp_size = {qp.fp_size}
AND offset_calib = '{qp.offset_calib}'
AND dens_adj
"))

# aggregate mean and standard deviation for all errors
aggr.vars = c("p", "p_r", "e", "e_r", "e_a", "e_ra", "isp", "asn", "bic")
aggr.by = c("thresh")

d.mean = aggregate(
  d[aggr.vars],
  d[aggr.by],
  mean
)
d.sd = aggregate(
  d[aggr.vars],
  d[aggr.by],
  sd
)

# create legend labels for threshold (to prevent 100% from showing before 20%)
thresh_levels = sort(unique(d$thresh))
d$thresh_label <- factor(
  d$thresh,
  levels=thresh_levels,
  labels=as.character(thresh_levels)
)


# custom colors and line styles for thresholds
ln.col = rep(c("#6600ff","#339933","#00ccff","#990033","#996633"), each=2)
ln.stl = rep(c("dotted","dashed","solid"), 3)

# local plotting parameters
xo = 35
lgd.title = "Retracker Threshold (%)"
lb.count = "Number of ASIRAS returns"
lb.density = "Distribution Density"


(
  ggplot(d, aes(x=p_r))
  + geom_histogram(bins=100, alpha=0.7)
  + geom_vline(xintercept=0, linetype="dashed")
  + annotate(
    "text", -xo, 2600,
    label="Snow\nSurface"
  )
  + geom_vline(xintercept=100)
  + annotate(
    "text", 100+xo, 2600,
    label="Ice\nSurface"
  )
  + xlab(lb.p_r)
  + ylab(lb.count)
  + coord_cartesian(c(-150, 200))
)

save.plot("p_r_hist", 1.2, 0.8)

(
  ggplot(d, aes(p_r, color=thresh_label, linetype = thresh_label))
  + geom_line(stat="density", size=1)
  + geom_vline(xintercept=0, linetype="dashed")
  + annotate(
    "text", -xo, 0.006,
    label="Snow\nSurface"
  )
  + geom_vline(xintercept=100)
  + annotate(
    "text", 100+xo, 0.006,
    label="Ice\nSurface"
  )
  + xlab(lb.p_r)
  + ylab(lb.density)
  + coord_cartesian(c(-100, 150))
  + scale_color_manual(values=ln.col)
  + scale_linetype_manual(values=ln.stl)
  + labs(color=lgd.title , linetype=lgd.title)
  + theme(legend.position = "bottom")
)

save.plot("p_r_dens_thresh", 1.2, 0.8)
