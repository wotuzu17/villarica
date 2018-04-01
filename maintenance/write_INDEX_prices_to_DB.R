# script to write index quotes to database
# -> NASDAQOMX/COMP
# 2018-03-01 va first working version

source("/home/voellenk/.quandlapikey.R")     # secret key file
# load libraries
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))
suppressPackageStartupMessages(library(Quandl))

this.date <- Sys.Date()

Quandl.api_key(api_key)

# retrieve NASDAQ Composite
COMP <- Quandl("NASDAQOMX/COMP", api_key=api_key)
COMP <- COMP[,c(1:4)]
colnames(COMP) <- c("date", "close", "high", "low")

COMP <- COMP[with(COMP, order(date)),]

# connect to database (keys are in secret file)
con <- dbConnect(MySQL(), user=dbd$user, password=dbd$password, dbname=dbd$db, host=dbd$host)

# create table if not exists
sql <- "CREATE TABLE IF NOT EXISTS `INDEXES` (
  `sym` varchar(10) NOT NULL,
  `date` date NOT NULL,
  `close` decimal(13,2) NOT NULL,
  `high` decimal(13,2) NOT NULL,
  `low` decimal(13,2) NOT NULL,
  PRIMARY KEY (`sym`, `date`)
) ENGINE=MyISAM DEFAULT CHARSET=ascii;"

try(dbSendQuery(con, sql))

# insert each row of COMP
for (i in 1:nrow(COMP)) {
  cat('*')
  sql <- sprintf("INSERT INTO `villarica`.`INDEXES` 
                  (`sym`, `date`, `close`, `high`, `low`) VALUES 
                  ('%s', '%s', '%f', '%f', '%f')", "COMP", 
                 as.character(COMP[i, "date"]), COMP[i,"close"], COMP[i,"high"], COMP[i,"low"])
  try(dbSendQuery(con, sql))
}

dbDisconnect(con)
