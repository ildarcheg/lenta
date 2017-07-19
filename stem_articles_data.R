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
StemArticlesData <- function() {

  dfM <- fread(file.path(tidyArticlesFolder, "tidy_articles_data.csv"), stringsAsFactors = FALSE, encoding = "UTF-8")
  dfM <- dfM[, -c("V1")]
  dt <- dfM %>% as.tbl() %>% select(urlKey, stemTitle, stemMetaDescription, stemPlaintext)  

  dt <- dt[, ]
  columnToStem <- "stemTitle"
  sourceFile <- file.path(stemedArticlesFolder, "stem_titles.csv")
  stemedFile <- file.path(stemedArticlesFolder, "stemed_titles.csv")
  write.csv(dt[, c("urlKey", columnToStem)], sourceFile, fileEncoding = "UTF-8")
  system(paste0("mystem -nlc ", sourceFile, " ", stemedFile), intern = FALSE)
  
  columnToStem <- "stemMetaDescription"
  sourceFile <- file.path(stemedArticlesFolder, "stem_metadescriptions.csv")
  stemedFile <- file.path(stemedArticlesFolder, "stemed_metadescriptions.csv")
  write.csv(dt[, c("urlKey", columnToStem)], sourceFile, fileEncoding = "UTF-8")
  system(paste0("mystem -nlc ", sourceFile, " ", stemedFile), intern = FALSE)
 
  columnToStem <- "stemPlaintext"
  sourceFile <- file.path(stemedArticlesFolder, "stem_plaintext.csv")
  stemedFile <- file.path(stemedArticlesFolder, "stemed_plaintext.csv")
  write.csv(dt[, c("urlKey", columnToStem)], sourceFile, fileEncoding = "UTF-8")
  system(paste0("mystem -nlc ", sourceFile, " ", stemedFile), intern = FALSE)

  res<- readLines(file.path(dataFolder, "stemed_plaintext.csv"), warn = FALSE, encoding = "UTF-8")
  res <- gsub("[{}]", "", res)
  res <- gsub("(\\|[^ ]+)", "", res)
  res <- gsub("\\?", "", res)
  res <- gsub("\\s+", " ", res)

}