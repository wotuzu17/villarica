# script to write Unicorn Research Corp data to database
# "Number of Stocks with Prices Unchanged, 52w high, ...
# 2018-03-01 va started work
# 2018-03-02 va first working version

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

cnames <- c("DEC", "UNCH", "ADV", "DEC_VOL", "UNCH_VOL", "ADV_VOL", "52W_LO", "52W_HI")
CODE <- paste("URC/NASDAQ", cnames, sep="_")

Quandl.api_key(api_key)

# retrieve 
data <- list()
for (i in 1:length(CODE)) {
  data[[i]] <- Quandl(CODE[i])
}

# connect to database (keys are in secret file)
con <- dbConnect(MySQL(), user=dbd$user, password=dbd$password, dbname=dbd$db, host=dbd$host)

# create table if not exists
sql <- "CREATE TABLE IF NOT EXISTS `URC` (
  `sym` varchar(10) NOT NULL,
  `date` date NOT NULL,
  `DEC` int(10) UNSIGNED DEFAULT NULL,
  `UNCH` int(10) UNSIGNED DEFAULT NULL,
  `ADV` int(10) UNSIGNED DEFAULT NULL,
  `DEC_VOL` float DEFAULT NULL,
  `UNCH_VOL` float DEFAULT NULL,
  `ADV_VOL` float DEFAULT NULL,
  `52W_LO` int(10) UNSIGNED DEFAULT NULL,
  `52W_HI` int(10) UNSIGNED DEFAULT NULL,
  PRIMARY KEY (`sym`, `date`)
) ENGINE=MyISAM DEFAULT CHARSET=ascii;"
try(dbSendQuery(con, sql))

# insert each row of COMP
for (i in 1:length(data)) {
  cat(i)
  # fill beginning with last
  r <- nrow(data[[i]])
  ALT <- ifelse(i %in% grep("_VOL", cnames), TRUE, FALSE)
  for (j in r:1) {
    this.date <- as.character(data[[i]][j,1])
    # check if sym/date exists
    sql <- sprintf("SELECT `sym` FROM `villarica`.`URC` WHERE `sym` = 'NASDAQ' AND `date` = '%s'", this.date)
    ans <- dbGetQuery(con, sql)
    if (nrow(ans) == 0) {
      # create row with sym/date
      sql <- sprintf("INSERT INTO `URC` (`sym`, `date`, `DEC`, `UNCH`, `ADV`, `DEC_VOL`, `UNCH_VOL`, `ADV_VOL`, `52W_LO`, `52W_HI`) 
              VALUES ('NASDAQ', '%s', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",this.date)
      dbSendQuery(con, sql)
    }
    # update existing sym/date row
    if (ALT ==  FALSE) {
      # feed integer value
      sql <- sprintf("UPDATE `URC` SET `%s` = '%d' WHERE `URC`.`sym` = 'NASDAQ' AND `URC`.`date` = '%s'", 
                     cnames[i], data[[i]][j,2], this.date)
    } else {
      # feed float value for volume columns
      sql <- sprintf("UPDATE `URC` SET `%s` = '%s' WHERE `URC`.`sym` = 'NASDAQ' AND `URC`.`date` = '%s'", 
                     cnames[i], format(data[[i]][j,2], scientific=TRUE), this.date)
    }
    dbSendQuery(con, sql)
  }
}

dbDisconnect(con)
