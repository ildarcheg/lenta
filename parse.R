args <- commandArgs(TRUE)
n <- as.integer(args[1])

setwd("C:/Users/ildar/lenta/")
source("get_lenta_articles_list.R")

ReadFilesInFolder(n)

