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
  dfM <- fread(file.path(tidyArticlesFolder, "tidy_articles_data.csv"), 
               stringsAsFactors = FALSE, encoding = "UTF-8")
  dt <- dfM %>% as.tbl() %>% 
    mutate(sep = "===") %>%
    select(sep, X, stemTitle, stemMetaDescription, stemPlaintext)
  
  sectionList <- list()
  sectionList[[1]] <- list(columnToStem = "stemTitle", 
                           stemedColumn = "stemedTitle",
                           sourceFile = file.path(stemedArticlesFolder, 
                                                  "stem_titles.txt"),
                           stemedFile = file.path(stemedArticlesFolder, 
                                                  "stemed_titles.txt"))
  sectionList[[2]] <- list(columnToStem = "stemMetaDescription", 
                           stemedColumn = "stemedMetaDescription",
                           sourceFile = file.path(stemedArticlesFolder, 
                                                  "stem_metadescriptions.txt"),
                           stemedFile = file.path(stemedArticlesFolder, 
                                                  "stemed_metadescriptions.txt"))
  sectionList[[3]] <- list(columnToStem = "stemPlaintext", 
                           stemedColumn = "stemedPlaintext",
                           sourceFile = file.path(stemedArticlesFolder, 
                                                  "stem_plaintext.txt"),
                           stemedFile = file.path(stemedArticlesFolder, 
                                                  "stemed_plaintext.txt"))
  
  for (i in 1:length(sectionList)) {
    print(paste0(i, " ", sectionList[[i]]$columnToStem, " ", Sys.time()))
    write.table(dt[, c("sep","X", sectionList[[i]]$columnToStem)], 
                sectionList[[i]]$sourceFile, 
                fileEncoding = "UTF-8", sep = ",", quote = FALSE, 
                row.names = FALSE, col.names = FALSE)
    system(paste0("mystem -nlc ", sectionList[[i]]$sourceFile, " ", 
                  sectionList[[i]]$stemedFile), intern = FALSE)  
  }
  
  for (i in 1:length(sectionList)) {
    print(paste0(i, " -1- ", sectionList[[i]]$stemedColumn, " ", Sys.time()))
    
    stemedText <- readLines(sectionList[[i]]$stemedFile, 
                            warn = FALSE, 
                            encoding = "UTF-8")
    print(paste0(i, " -2- ", sectionList[[i]]$stemedColumn, " ", Sys.time()))
    chunkList <- chunk(stemedText, chunk.size = 10000000)
    
    print(paste0(i, " -3- ", sectionList[[i]]$stemedColumn, " ", Sys.time()))
    resLines <- c()
    for (j in 1:length(chunkList)) {
      print(paste0(i, " -3- ", j, " ", sectionList[[i]]$stemedColumn, " ", Sys.time()))
      resTemp <- chunkList[[j]] %>% 
        str_replace_all("===,", "===") %>%
        strsplit(split = "\\\\n|,") %>% unlist() %>% 
        str_replace_all("(\\|[^ ]+)|(\\\\[^ ]+)|\\?|,|_", "")
      resLines <- c(resLines, resTemp[resTemp!=""])
    }  
    
    print(paste0(i, " -4- ", sectionList[[i]]$stemedColumn, " ", Sys.time()))
    chunkedRes <- chunk(resLines, chunk.delimiter = "===", 
                        fixed.delimiter = FALSE, 
                        keep.delimiter = TRUE)
    print(paste0(i, " -5- ", sectionList[[i]]$stemedColumn, " ", Sys.time()))
    stemedList <- lapply(chunkedRes, 
                         function(x) {
                           data.frame(key = as.integer(str_replace_all(x[1], "===", "")), 
                                      content = paste0(x[2:length(x)], collapse = " "), 
                                      stringsAsFactors = FALSE)})
    print(paste0(i, " -6- ", sectionList[[i]]$stemedColumn, " ", Sys.time()))
    sectionList[[i]]$dt <- bind_rows(stemedList)
  }
  
  dt <- dfM %>% as.tbl() %>% 
    mutate(key = X)
  
  dt <- left_join(dt, sectionList[[1]]$dt, by = "key")
  dt <- left_join(dt, sectionList[[2]]$dt, by = "key")
  dt <- left_join(dt, sectionList[[2]]$dt, by = "key")
  
}