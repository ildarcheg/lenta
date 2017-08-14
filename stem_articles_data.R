require(data.table)
require(dplyr)
require(tidyr)
require(stringr)

# Set workling directory and locale for macOS and Windows
if (Sys.info()['sysname'] == "Windows") {
  workingDirectory <- paste0(Sys.getenv("HOMEPATH"), "\\lenta") 
  Sys.setlocale("LC_ALL", "Russian")
} else {
  workingDirectory <- ("~/lenta")
  Sys.setlocale("LC_ALL", "ru_RU.UTF-8")
}
setwd(workingDirectory)
source("chunk.R")

# Set common variables
tidyArticlesFolder <- file.path(getwd(), "tidy_articles")
stemedArticlesFolder <- file.path(getwd(), "stemed_articles")

# Create required folders if not exist 
dir.create(stemedArticlesFolder, showWarnings = FALSE)

## STEP 6. Stem title, description and plain text
# Write columns on disk, run mystem, read stemed data and add to data.table
StemArticlesData <- function() {
  
  dfM <- fread(file.path(tidyArticlesFolder, "tidy_articles_data.csv"), stringsAsFactors = FALSE, encoding = "UTF-8")
  #dfM <- read.csv(file.path(tidyArticlesFolder, "tidy_articles_data.csv"), stringsAsFactors = FALSE, encoding = "UTF-8")
  dt <- dfM %>% as.tbl() %>% 
    select(urlKey, stemTitle, stemMetaDescription, stemPlaintext) %>% 
    mutate(urlKey = gsub("_|-", "", urlKey)) %>%
    mutate(urlKey = paste0("=====", urlKey, "==="))
  
  print(paste0("1 ", Sys.time()))
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

  CleanStemedText <- function(stemedText) {
    chunkList <- chunk(stemedText, chunk.size = 10000000)
    resLines <- c()
    print(length(chunkList))
    for (i in 1:length(chunkList)) {
      resTemp <- chunkList[[i]] %>% 
        strsplit(split = "\\\\n|===") %>% unlist() %>% 
        str_replace_all("(\\|[^ ]+)|(\\\\[^ ]+)|\\?|,|_|===", "")
      resLines <- c(resLines, resTemp[resTemp!=""])
    }  
    return(resLines)
  }
  
  res <- readLines(file.path(stemedArticlesFolder, "stemed_titles.txt"), warn = FALSE, encoding = "UTF-8")
  resLines <- CleanStemedText(res)
  
  print(paste0("5 ", Sys.time()))
  
  chunkedRes <- chunk(resLines, chunk.delimiter = "==http", fixed.delimiter = FALSE, keep.delimiter = TRUE)
  stemedList <- lapply(chunkedRes, function(x) {data.frame(urlKey = x[1], content = paste0(x[2:length(x)], collapse = " "), stringsAsFactors = FALSE)})
  
  dtTitles <- bind_rows(stemedList)
  dtTitles$type <- "stemedTitles"
  
  print(paste0("6 ", Sys.time()))
  
  res <- readLines(file.path(stemedArticlesFolder, "stemed_metadescriptions.txt"), warn = FALSE, encoding = "UTF-8")
  resLines <- CleanStemedText(res)
  
  print(paste0("7 ", Sys.time()))
  
  chunkedRes <- chunk(resLines, chunk.delimiter = "==http", fixed.delimiter = FALSE, keep.delimiter = TRUE)
  stemedList <- lapply(chunkedRes, function(x) {data.frame(urlKey = x[1], content = paste0(x[2:length(x)], collapse = " "), stringsAsFactors = FALSE)})
  
  dtMetadescriptions <- bind_rows(stemedList)
  dtMetadescriptions$type <- "stemedMetadescriptions"
  
  print(paste0("8 ", Sys.time()))
  
  res <- readLines(file.path(stemedArticlesFolder, "stemed_plaintext.txt"), warn = FALSE, encoding = "UTF-8")
  resLines <- CleanStemedText(res)
  
  print(paste0("9 ", Sys.time()))
  
  chunkedRes <- chunk(resLines, chunk.delimiter = "==http", fixed.delimiter = FALSE, keep.delimiter = TRUE)
  stemedList <- lapply(chunkedRes, function(x) {data.frame(urlKey = x[1], content = paste0(x[2:length(x)], collapse = " "), stringsAsFactors = FALSE)})
  
  dtPlaintext <- bind_rows(stemedList)
  dtPlaintext$type <- "stemedPlainText"
  
  print(paste0("10 ", Sys.time()))
  
}