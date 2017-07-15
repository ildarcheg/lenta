require(data.table)

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
  dfM <- fread(file.path(dataFolder, "tidy_articles_data.csv"), stringsAsFactors = FALSE, encoding = "UTF-8")
}