# script to test quandl api

source("~/.quandlapikey.R")

# load required libraries
library(Quandl)
library(quantmod)
library(xts)

Quandl.api_key(api_key)

# list of tickers in WIKI database
# https://www.quandl.com/api/v3/databases/WIKI/codes?api_key=6ciz2MPm6RU_zJkAuAXm
temp <- tempfile()
download.file(sprintf('https://www.quandl.com/api/v3/databases/WIKI/codes?%s', api_key), temp)
symbols <- read.csv(unz(temp, "WIKI-datasets-codes.csv"), header=FALSE)
unlink(temp)

colnames(symbols) <- c("APICODE", "original_desc")
symbols$TICKER <- sub(".*/", "", symbols[,1])
symbols$DESCRIPTION <- sub(").*$", ")", symbols[,2])

oneday <- Quandl.datatable('WIKI/PRICES', date='1999-11-18', ticker='A')
#   ticker       date open high low close   volume ex-dividend split_ratio adj_open adj_high  adj_low adj_close adj_volume
# 1      A 1999-11-18 45.5   50  40    44 44739900           0           1 31.04195 34.11203 27.28963  30.01859   44739900

frombegin <- Quandl.datatable('WIKI/PRICES', ticker='A')
