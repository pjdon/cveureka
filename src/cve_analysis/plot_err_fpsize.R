# Error value vs. footprint size

source("config.r")

d = sql.fetch(sql.query("
SELECT fp_size fp,{sq.error_stat}
FROM {db.schema}.asr_error
JOIN {db.schema}.asr_grid_zone USING (id_asr)
WHERE dens_adj
AND offset_calib = '{qp.offset_calib}'
GROUP BY fp_size
"))

dcont = d[d$fp>0,]
dpdl = d[d$fp==-1,]

pdl_mean = dpdl$e_ra_mean
pdl_sd = dpdl$e_ra_sd
pdl_label = "Pulse-Doppler Limited Footprint"

ggarrange(
  (
    ggplot(dcont, aes(x=fp, y=e_ra_mean))
    + geom_point(size=2)
    + geom_hline(aes(yintercept=pdl_mean))
    + annotate("text", 22, pdl_mean, vjust=-1.5, label=pdl_label)
    + geom_smooth(method=lm, formula=y~poly(x, 4, raw=TRUE), se = FALSE)
    + xlab(lb.fp_size)
    + ylab(lb.e_ra_mean)
  ),
  (
    ggplot(dcont, aes(x=fp, y=e_ra_sd))
    + geom_point(size=2)
    + geom_hline(aes(yintercept=pdl_sd))
    + annotate("text", 22, pdl_sd, vjust=2, label=pdl_label)
    + geom_smooth(method=lm, formula=y~poly(x, 4, raw=TRUE), se = FALSE)
    + xlab(lb.fp_size)
    + ylab(lb.e_ra_sd)
  ),
  labels = 'AUTO'
)

save.plot("e_ra_fpsize", 2)
