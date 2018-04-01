# script to write Unicorn Research Corp data to database
# "Number of Stocks with Prices Unchanged, 52w high, ...
# 2018-03-01 va started work

# URC/NASDAQ_DEC      NASDAQ: Number of Stocks with Prices Declining
# URC/NASDAQ_UNCH     NASDAQ: Number of Stocks with Prices Unchanged
# URC/NASDAQ_ADV      NASDAQ: Number of Stocks with Prices Advancing
# URC/NASDAQ_DEC_VOL  NASDAQ: Volume of Stocks with Prices Declining
# URC/NASDAQ_UNCH_VOL NASDAQ: Volume of Stocks with Prices Unchanged
# URC/NASDAQ_ADV_VOL  NASDAQ: Volume of Stocks with Prices Advancing
# URC/NASDAQ_52W_LO   NASDAQ: Number of Stocks Making 52-Week Lows
# URC/NASDAQ_52W_HI   NASDAQ: Number of Stocks Making 52-Week Highs


source("/home/voellenk/.quandlapikey.R")     # secret key file
# load libraries
suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(RMySQL))
suppressPackageStartupMessages(library(Quandl))

CODE <- paste("URC/NASDAQ", c("DEC", "UNCH", "ADV", "DEV_VOL", "UNCH_VOL", "ADV_VOL", "52W_LO", "52W_HI"), sep="_")

Quandl.api_key(api_key)

# retrieve 

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
