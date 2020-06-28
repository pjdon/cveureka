# Imports ----
require(ggplot2)
require(ggpubr)
require(ggcorrplot)
require(latex2exp)
require(ini)
require(reshape2)
require(RPostgreSQL)


library(stringr)

# Configuration ----

# Returns the configuration structure
# Tries to get the file directly first and then asks if that fails
get.config = function(dir.path, file.name, file.filter) {
  config = tryCatch(
    
    {
      try.path = file.path(dir.path, file.name)
      read.ini(try.path)
    },
    
    error = function(e) {
      
      try.dir = file.path(dir.path, file.filter)
      alt.path = choose.files(
        default=try.dir,
        caption="Select config.ini",
        multi=FALSE
      )
      print(alt.path)
      read.ini(alt.path)
    }
    
  )
  
  return(config)
}

# Plotting ----

# Returns a function that saves the current plot to a PNG of `name` to `path`
makefunc.save.plot <- function(
  prefix, path, default.ratio=1.4, default.scale=1.1, default.width=8
){
  return(
    function(
      name=NULL, ratio=default.ratio, scale=default.scale,
      width=default.width
    ){
      
      # width is standardized so that text is generally the same size
      # throughout the whole document
      height=width/ratio
      
      ggsave(
        sprintf("%s%s.png", prefix, name),
        path=path,
        height=height,
        width=width,
        scale=scale
      )
    }
  )
}

# SQL ----



# Replaces items in {...} braces with local variables
# and replaces backticks ` with " to use as identifiers
sql.query <- function(string) {
  return(stringr::str_glue(gsub("`", "\"", string)))
}

# Closes all PostgreSQL connections
sql.closeAllConnections <- function(driver) {
  lapply(
    dbListConnections(drv = driver),
    function(x) {dbDisconnect(conn = x)}
  )
}

# Execute, fetch and return the result of `query` using an ad-hoc connection
# This prevents the buildup of open connections during testing
makefunc.sql.fetch <- function(driver, dbname, host, port, user, password) {
  return (
    function(query) {
      con = dbConnect(
        driver,
        dbname = dbname,
        host = host,
        port = port,
        user = user,
        password = password
      )
      on.exit(dbDisconnect(con))
      result = dbGetQuery(con, query)
      if (dim(result)[1] == 0 || dim(result)[2] == 0) {
        warning("query result is empty")
      }
      return(result)
    }
  )
}

# Factors ----
style.factors <- function(factors, template="%s to %s") {
  parts = str_split(str_sub(factors, 2, -2), ",", simplify=TRUE)
  return(sprintf(template, parts[,1], parts[,2]))
}

pretty.cut <- function(data, ncuts, template="%s to %s") {
  temp = cut(data, ncuts)
  levels(temp) = style.factors(levels(temp), template)
  return(temp)
}

# Statistics ----

mode <- function(data) {
  x<-data
  lim.inf=min(x)-1; lim.sup=max(x)+1
  
  s<-density(x,from=lim.inf,to=lim.sup,bw=0.2)
  n<-length(s$y)
  v1<-s$y[1:(n-2)];
  v2<-s$y[2:(n-1)];
  v3<-s$y[3:n]
  ix<-1+which((v1<v2)&(v2>v3))
  
  md <- s$x[which(s$y==max(s$y))] 
  
  return(md)
}
