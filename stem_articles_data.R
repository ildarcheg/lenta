require(data.table)
require(dplyr)
require(tidyr)

# Set workling directory and locale for macOS and Windows
if (Sys.info()['sysname'] == "Windows") {
  workingDirectory <- paste0(Sys.getenv("HOMEPATH"), "\\lenta") 
  Sys.setlocale("LC_ALL", "Russian")
} else {
  workingDirectory <- ("~/lenta")
  Sys.setlocale("LC_ALL", "ru_RU.UTF-8")
}
setwd(workingDirectory)

# Set common variables
tidyArticlesFolder <- file.path(getwd(), "tidy_articles")
stemedArticlesFolder <- file.path(getwd(), "stemed_articles")

# Creare required folders if not exist 
dir.create(stemedArticlesFolder, showWarnings = FALSE)

## STEP 6. Stem title, description and plain text
# Write columns on disk, run mystem, read stemed data and add to data.table
CreateCommandFileForStemData <- function() {

  dfM <- fread(file.path(tidyArticlesFolder, "tidy_articles_data.csv"), stringsAsFactors = FALSE, encoding = "UTF-8")
  dfM <- dfM[, -c("V1")]
  dt <- dfM %>% as.tbl() %>% select(urlKey, stemTitle, stemMetaDescription, stemPlaintext)  

  dt <- dt[, ]
  write.csv(dt[, c("urlKey", "stemTitle")], 
    file.path(dataFolder, "stem_titles.csv"), fileEncoding = "UTF-8")
  system("mystem -nlc data/stem_titles.csv data/stemed_titles.csv", intern = FALSE)
  write.csv(dt[ , c("urlKey", "stemMetaDescription")], 
    file.path(dataFolder, "stem_metadescriptions.csv"), fileEncoding = "UTF-8")
  system("mystem -nlc data/stem_metadescriptions.csv data/stemed_metadescriptions.csv", intern = FALSE)
  write.csv(dt[ , c("urlKey", "stemPlaintext")], 
    file.path(dataFolder, "stem_plaintext.csv"), fileEncoding = "UTF-8")
  system("mystem -nlc data/stem_plaintext.csv data/stemed_plaintext.csv", intern = FALSE)

  res<- readLines(file.path(dataFolder, "stemed_plaintext.csv"), warn = FALSE, encoding = "UTF-8")
  res <- gsub("[{}]", "", res)
  res <- gsub("(\\|[^ ]+)", "", res)
  res <- gsub("\\?", "", res)
  res <- gsub("\\s+", " ", res)

}