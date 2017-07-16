require(data.table)
require(stringr)
require(tm)

Sys.setlocale("LC_ALL", "ru_RU.UTF-8")

# setting working directory for mac and win
if (Sys.getenv("HOMEPATH") == "") {
  workingDirectory <- ("~/lenta")
} else {
  workingDirectory <- paste0(Sys.getenv("HOMEPATH"), "\\lenta")  
}
setwd(workingDirectory)

CreateCommandFileForStemData <- function() {
  dataFolder <- file.path(getwd(), "data")
  stopWords <- readLines(file.path(dataFolder, "stop_words.csv"), warn = FALSE, encoding = "UTF-8")
  dfM <- fread(file.path(dataFolder, "tidy_articles_data.csv"), stringsAsFactors = FALSE, encoding = "UTF-8")
  
  dt <- dfM %>% select(url, title, metaDescription, plaintext, imageDescription, videoDescription)
  write.csv(dt[1:10, ], file.path(dataFolder, "sample_articles_data.csv"), fileEncoding = "UTF-8")
}