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
  dfM <- read.csv(file.path(dataFolder, "untidy_articles_data.csv"), stringsAsFactors = FALSE, encoding = "UTF-8")
  
  dtM <- as.tbl(dfM)
  dtD <- dtM %>% select(-X.1,-X) %>% distinct(url, .keep_all=TRUE) 
  
    
  na.omit(dtD,cols="url")
  
}
