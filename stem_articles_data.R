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
  
  print(paste0("Start ", Sys.time()))
  dfM <- fread(file.path(tidyArticlesFolder, "tidy_articles_data.csv"), stringsAsFactors = FALSE, encoding = "UTF-8")
  #dfM <- read.csv(file.path(tidyArticlesFolder, "tidy_articles_data.csv"), stringsAsFactors = FALSE, encoding = "UTF-8")
  dt <- dfM %>% as.tbl() %>% 
    mutate(sep = "===") %>%
    select(sep, X, stemTitle, stemMetaDescription, stemPlaintext)
  
  sectionList <- list()
  sectionList[[1]] <- list(columnToStem = "stemTitle", 
                           stemedColumn = "stemedTitle",
                           sourceFile = file.path(stemedArticlesFolder, "stem_titles.txt"),
                           stemedFile = file.path(stemedArticlesFolder, "stemed_titles.txt"))
  sectionList[[2]] <- list(columnToStem = "stemMetaDescription", 
                           stemedColumn = "stemedMetaDescription",
                           sourceFile = file.path(stemedArticlesFolder, "stem_metadescriptions.txt"),
                           stemedFile = file.path(stemedArticlesFolder, "stemed_metadescriptions.txt"))
  sectionList[[3]] <- list(columnToStem = "stemPlaintext", 
                           stemedColumn = "stemedPlaintext",
                           sourceFile = file.path(stemedArticlesFolder, "stem_plaintext.txt"),
                           stemedFile = file.path(stemedArticlesFolder, "stemed_plaintext.txt"))
  
  for (i in 1:length(sectionList)) {
    print(paste0(i, " ", sectionList[[i]]$columnToStem, " ", Sys.time()))
    write.table(dt[, c("sep","X", sectionList[[i]]$columnToStem)], 
                sectionList[[i]]$sourceFile, 
                fileEncoding = "UTF-8", sep = ",", quote = FALSE, 
                row.names = FALSE, col.names = FALSE)
    system(paste0("mystem -nlc ", sectionList[[i]]$sourceFile, " ", sectionList[[i]]$stemedFile), intern = FALSE)  
  }
  
  # print(paste0("1 ", Sys.time()))
  # columnToStem <- "stemTitle"
  # sourceFile <- file.path(stemedArticlesFolder, "stem_titles.txt")
  # stemedFile <- file.path(stemedArticlesFolder, "stemed_titles.txt")
  # write.table(dt[, c("sep","X", columnToStem)], sourceFile, 
  #             fileEncoding = "UTF-8", sep = ",", quote = FALSE, 
  #             row.names = FALSE, col.names = FALSE)
  # system(paste0("mystem -nlc ", sourceFile, " ", stemedFile), intern = FALSE)
  # 
  # print(paste0("2 ", Sys.time())) 
  # columnToStem <- "stemMetaDescription"
  # sourceFile <- file.path(stemedArticlesFolder, "stem_metadescriptions.txt")
  # stemedFile <- file.path(stemedArticlesFolder, "stemed_metadescriptions.txt")
  # write.table(dt[, c("sep","X", columnToStem)], sourceFile, 
  #             fileEncoding = "UTF-8", sep = ",", quote = FALSE, 
  #             row.names = FALSE, col.names = FALSE)
  # system(paste0("mystem -nlc ", sourceFile, " ", stemedFile), intern = FALSE)
  # 
  # print(paste0("3 ", Sys.time()))
  # columnToStem <- "stemPlaintext"
  # sourceFile <- file.path(stemedArticlesFolder, "stem_plaintext.txt")
  # stemedFile <- file.path(stemedArticlesFolder, "stemed_plaintext.txt")
  # write.table(dt[, c("sep","X", columnToStem)], sourceFile, 
  #             fileEncoding = "UTF-8", sep = ",", quote = FALSE, 
  #             row.names = FALSE, col.names = FALSE)
  # system(paste0("mystem -nlc ", sourceFile, " ", stemedFile), intern = FALSE)
  
  print(paste0("4 ", Sys.time()))

  CleanStemedText <- function(stemedText) {
    chunkList <- chunk(stemedText, chunk.size = 10000000)
    resLines <- c()
    print(length(chunkList))
    for (i in 1:length(chunkList)) {
      resTemp <- chunkList[[i]] %>% 
        str_replace_all("===,", "===") %>%
        strsplit(split = "\\\\n|,") %>% unlist() %>% 
        str_replace_all("(\\|[^ ]+)|(\\\\[^ ]+)|\\?|,|_", "")
      resLines <- c(resLines, resTemp[resTemp!=""])
    }  
    return(resLines)
  }
  
  res <- readLines(file.path(stemedArticlesFolder, "stemed_titles.txt"), warn = FALSE, encoding = "UTF-8")
  resLines <- CleanStemedText(res)
  
  print(paste0("5 ", Sys.time()))
  
  chunkedRes <- chunk(resLines, chunk.delimiter = "===", fixed.delimiter = FALSE, keep.delimiter = TRUE)
  stemedList <- lapply(chunkedRes, function(x) {data.frame(key = as.integer(str_replace_all(x[1], "===", "")), content = paste0(x[2:length(x)], collapse = " "), stringsAsFactors = FALSE)})
  
  dtTitles <- bind_rows(stemedList)
  dtTitles$type <- "stemedTitles"
  
  print(paste0("6 ", Sys.time()))
  
  res <- readLines(file.path(stemedArticlesFolder, "stemed_metadescriptions.txt"), warn = FALSE, encoding = "UTF-8")
  resLines <- CleanStemedText(res)
  
  print(paste0("7 ", Sys.time()))
  
  chunkedRes <- chunk(resLines, chunk.delimiter = "===", fixed.delimiter = FALSE, keep.delimiter = TRUE)
  stemedList <- lapply(chunkedRes, function(x) {data.frame(urlKey = str_replace_all(x[1], "===", ""), content = paste0(x[2:length(x)], collapse = " "), stringsAsFactors = FALSE)})
  
  dtMetadescriptions <- bind_rows(stemedList)
  dtMetadescriptions$type <- "stemedMetadescriptions"
  
  print(paste0("8 ", Sys.time()))
  
  res <- readLines(file.path(stemedArticlesFolder, "stemed_plaintext.txt"), warn = FALSE, encoding = "UTF-8")
  resLines <- CleanStemedText(res)
  
  print(paste0("9 ", Sys.time()))
  
  chunkedRes <- chunk(resLines, chunk.delimiter = "===", fixed.delimiter = FALSE, keep.delimiter = TRUE)
  stemedList <- lapply(chunkedRes, function(x) {data.frame(urlKey = str_replace_all(x[1], "===", ""), content = paste0(x[2:length(x)], collapse = " "), stringsAsFactors = FALSE)})
  
  dtPlaintext <- bind_rows(stemedList)
  dtPlaintext$type <- "stemedPlainText"
  
  dt <- left_join(dtTitles, dtMetadescriptions, by = "urlKey")
                  
  print(paste0("10 ", Sys.time()))
  
}