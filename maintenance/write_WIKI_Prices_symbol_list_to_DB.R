# script to write list of symbols from quandl 'WIKI Prices" to database
# 2018-03-01 va first working version

source("/home/voellenk/.quandlapikey.R")     # secret key file
# load libraries
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))
suppressPackageStartupMessages(library(Quandl))

this.date <- Sys.Date()

Quandl.api_key(api_key)

# list of tickers in WIKI database
# https://www.quandl.com/api/v3/databases/WIKI/codes?api_key=6ciz2MPm6RU_zJkAuAXm
temp <- tempfile()
download.file(sprintf('https://www.quandl.com/api/v3/databases/WIKI/codes?%s', api_key), temp)
symbols <- read.csv(unz(temp, "WIKI-datasets-codes.csv"), header=FALSE, stringsAsFactors = FALSE)
unlink(temp)

colnames(symbols) <- c("APICODE", "original_desc")
symbols$TICKER <- sub(".*/", "", symbols[,1])
symbols$DESCRIPTION <- sub("'", "", sub(").*$", ")", symbols[,2]))
symbols <- symbols[with(symbols, order(TICKER)),]

# connect to database (keys are in secret file)
con <- dbConnect(MySQL(), user=dbd$user, password=dbd$password, dbname=dbd$db, host=dbd$host)

# create table if not exists
sql <- sprintf("CREATE TABLE IF NOT EXISTS `WIKI_PRICES_Symbols_%s` (
  `TICKER` varchar(10) NOT NULL,
  `DESCRIPTION` varchar(100) NOT NULL,
  `APICODE` varchar(15) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=ascii;", as.character(this.date))
try(dbSendQuery(con, sql))

# insert each row of data.frame symbols
for (i in 1:nrow(symbols)) {
  cat('*')
  sql <- sprintf("INSERT INTO `villarica`.`WIKI_PRICES_Symbols_%s` 
                  (`TICKER`, `DESCRIPTION`, `APICODE`) VALUES 
                  ('%s', '%s', '%s')", 
                 as.character(this.date), symbols[i, "TICKER"], symbols[i, "DESCRIPTION"], symbols[i, "APICODE"])
  try(dbSendQuery(con, sql))
}

dbDisconnect(con)
