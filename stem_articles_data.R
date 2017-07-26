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
  
  print(paste0("1 ", Sys.time()))
  dt <- dt[, ]
  columnToStem <- "stemTitle"
  sourceFile <- file.path(stemedArticlesFolder, "stem_titles.txt")
  stemedFile <- file.path(stemedArticlesFolder, "stemed_titles.txt")
  write.table(dt[, c("urlKey", columnToStem)], sourceFile, 
              fileEncoding = "UTF-8", sep = ",", quote = FALSE, 
              row.names = FALSE, col.names = FALSE)
  system(paste0("mystem -nlc ", sourceFile, " ", stemedFile), intern = FALSE)
  
  print(paste0("2 ", Sys.time())) 
  columnToStem <- "stemMetaDescription"
  sourceFile <- file.path(stemedArticlesFolder, "stem_metadescriptions.txt")
  stemedFile <- file.path(stemedArticlesFolder, "stemed_metadescriptions.txt")
  write.table(dt[, c("urlKey", columnToStem)], sourceFile, 
              fileEncoding = "UTF-8", sep = ",", quote = FALSE, 
              row.names = FALSE, col.names = FALSE)
  system(paste0("mystem -nlc ", sourceFile, " ", stemedFile), intern = FALSE)
  
  print(paste0("3 ", Sys.time()))
  columnToStem <- "stemPlaintext"
  sourceFile <- file.path(stemedArticlesFolder, "stem_plaintext.txt")
  stemedFile <- file.path(stemedArticlesFolder, "stemed_plaintext.txt")
  write.table(dt[, c("urlKey", columnToStem)], sourceFile, 
              fileEncoding = "UTF-8", sep = ",", quote = FALSE, 
              row.names = FALSE, col.names = FALSE)
  system(paste0("mystem -nlc ", sourceFile, " ", stemedFile), intern = FALSE)
  
  print(paste0("4 ", Sys.time()))
  
  system.time(res<- readLines(file.path(stemedArticlesFolder, "stemed_titles.txt"), warn = FALSE, encoding = "UTF-8"))
  system.time(res <- gsub("[{}]", "", res))
  system.time(res <- gsub("(\\|[^ ]+)", "", res))
  system.time(res <- gsub("\\?|\\\\n|,", "", res))
  system.time(res <- gsub("\\s+", " ", res))
}