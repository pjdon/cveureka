# Error stats vs. threshold

source("config.r")

d = sql.fetch(sql.query("
SELECT
  (tfmra_threshold * 100)::int AS threshold,
  abs_rel_error * 100 AS e_ra,
  penetration * 100 AS p,
  rel_penetration * 100 AS p_r
FROM {db.schema}.asr_error
WHERE fp_size = {qp.fp_size}
AND dens_adj
AND offset_calib = '{qp.offset_calib}'
"))

d.mean = aggregate(
  d[c("p","p_r")],
  d[c("threshold")],
  mean
)

d.sd = aggregate(
  d[c("p","p_r")],
  d[c("threshold")],
  sd
)

(
  ggplot(d, aes(x=threshold, y=p_r, group=threshold))
  + stat_boxplot(geom="errorbar")
  + geom_boxplot()
  + labs(x=lb.threshold, y=lb.p_r)
)

save.plot("p_r_boxplot", 1.2, 0.8)


(
  ggplot()
  + geom_point(data=d.mean, aes(x=threshold, y=p_r))
  + geom_smooth(
    data=d.mean, aes(x=threshold, y=p_r),
    method=lm, formula=y~poly(x, 3, raw=TRUE), se=FALSE
  )
  + labs(x=lb.threshold, y=lb.p_r_mean)
)

save.plot("p_r_thresh", 1.2, 0.8)

