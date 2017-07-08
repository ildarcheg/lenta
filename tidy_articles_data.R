require(lubridate)
require(dplyr)

Sys.setlocale("LC_ALL", "ru_RU.UTF-8")

# setting working directory for mac and win
if (Sys.getenv("HOMEPATH") == "") {
  workingDirectory <- ("~/lenta")
} else {
  workingDirectory <- paste0(Sys.getenv("HOMEPATH"), "\\lenta")  
}
setwd(workingDirectory)

TityData <- function() {
  dataFolder <- file.path(getwd(), "data")
  df <- read.csv(file.path(dataFolder, "untidy_articles_data.csv"), stringsAsFactors = FALSE, encoding = "UTF-8")
  dt <- as.tbl(df)
  dt
  
}
