source("config.r")
require(reshape2)

d = sql.fetch(sql.query("
SELECT (tfmra_threshold*100) thresh,{sq.error}
FROM {db.schema}.asr_error
WHERE fp_size = {qp.fp_size}
AND offset_calib = '{qp.offset_calib}'
AND dens_adj
"))

aggr.vals = c("p_r", "isp", "asn", "bic")
aggr.id = c("thresh")

# aggregate by stats
d.mean = aggregate(d[aggr.vals],d[aggr.id],mean)
d.sd = aggregate(d[aggr.vals],d[aggr.id],sd)

d.stat = rbind(
  cbind(stat="Mean",d.mean),
  cbind(stat="Standard Deviation", d.sd)
)

# create legend labels for threshold (to prevent 100% from showing before 20%)
thresh_levels = sort(unique(d$thresh))
d$thresh_label <- factor(
  d$thresh,
  levels=thresh_levels,
  labels=as.character(thresh_levels)
)

# local plotting parameters
ann.xo = 20
ann.y = 0.006
lb.density = "Distribution Density"

ggarrange(
  (
    ggplot(
      subset(d, is.element(thresh_label, c("100", "20"))),
      aes(p_r)
    )
    + geom_histogram(aes(y=..density.., fill="Histogram\n(All Thresholds)"), bins=100, position="identity", alpha=0.3)
    + geom_line(aes(color=thresh_label), stat="density", size=1)
    + geom_vline(xintercept=0, linetype="dashed")
    + annotate(
      "text", -ann.xo, ann.y,
      label="Snow\nSurface"
    )
    + geom_vline(xintercept=100)
    + annotate(
      "text", 100+ann.xo, ann.y,
      label="Ice\nSurface"
    )
    + scale_fill_manual(name=NULL, values="black")
    + xlab(lb.p_r)
    + ylab(lb.density)
    + coord_cartesian(c(-100, 150))
    + labs(color="TFMRA\nThreshold (%)")
    + plthm
    + theme(legend.position = "bottom")
  ),
  (
    ggplot(data=d.stat, aes(x=thresh, y=p_r, color=stat))
    + geom_point(size=2)
    + geom_smooth(method=lm, formula=y~poly(x, 3, raw=TRUE), se=FALSE)
    + labs(x=lb.threshold, y=lb.p_r, color="")
    + plthm
    + theme(legend.position = "bottom")
  ),
  labels="AUTO"
)
  
save.plot("p_r_dist_stat", 2, 1.1)
