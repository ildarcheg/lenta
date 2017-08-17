require(data.table)
require(dplyr)
require(tidyr)
require(stringr)
require(gdata)

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
  
  timestamp(prefix = "## START reading file ")
  tidyDataFile <- file.path(tidyArticlesFolder, "tidy_articles_data.csv")
  dt <- fread(tidyDataFile, stringsAsFactors = FALSE, encoding = "UTF-8") %>% 
    as.tbl()
  dt <- dt %>% mutate(sep = "===") %>%
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
    timestamp(prefix = paste0("## steming file ", i, " ", sectionList[[i]]$columnToStem, " "))
    write.table(dt[, c("sep","X", sectionList[[i]]$columnToStem)], 
                sectionList[[i]]$sourceFile, 
                fileEncoding = "UTF-8", sep = ",", quote = FALSE, 
                row.names = FALSE, col.names = FALSE)
    system(paste0("mystem -nlc ", sectionList[[i]]$sourceFile, " ", 
                  sectionList[[i]]$stemedFile), intern = FALSE)  
  }
  
  rm(dt)
  
  ll(unit = "MB")
  
  for (i in 1:length(sectionList)) {
    timestamp(prefix = paste0("## parsing file ", i, " -1- ", sectionList[[i]]$stemedColumn, " "))
    stemedText <- readLines(sectionList[[i]]$stemedFile, 
                            warn = FALSE, 
                            encoding = "UTF-8")
    timestamp(prefix = paste0("## parsing file ", i, " -2- ", sectionList[[i]]$stemedColumn, " "))
    chunkList <- chunk(stemedText, chunk.size = 10000000)
    
    stemedText <- ""
    
    resLines <- c()
    for (j in 1:length(chunkList)) {
      timestamp(prefix = paste0("## parsing file ", i, " -3- ", j, " ", sectionList[[i]]$stemedColumn, " "))
      resTemp <- chunkList[[j]] %>% 
        str_replace_all("===,", "===") %>%
        strsplit(split = "\\\\n|,") %>% unlist() %>% 
        str_replace_all("(\\|[^ ]+)|(\\\\[^ ]+)|\\?|,|_", "")
      resLines <- c(resLines, resTemp[resTemp!=""])
    }  
    
    chunkList <- ""
    
    timestamp(prefix = paste0("## parsing file ", i, " -4- ", sectionList[[i]]$stemedColumn, " "))
    chunkedRes <- chunk(resLines, chunk.delimiter = "===", 
                        fixed.delimiter = FALSE, 
                        keep.delimiter = TRUE)
    
    resLines <- ""
    
    timestamp(prefix = paste0("## parsing file ", i, " -5- ", sectionList[[i]]$stemedColumn, " "))
    stemedList <- lapply(chunkedRes, 
                         function(x) {
                           data.frame(key = as.integer(str_replace_all(x[1], "===", "")), 
                                      content = paste0(x[2:length(x)], collapse = " "), 
                                      stringsAsFactors = FALSE)})
    
    chunkedRes <- ""
    
    timestamp(prefix = paste0("## parsing file ", i, " -6- ", sectionList[[i]]$stemedColumn, " "))
    sectionList[[i]]$dt <- bind_rows(stemedList)
    colnames(sectionList[[i]]$dt) <- c("key", sectionList[[i]]$stemedColumn)
    
    stemedList <- ""
    
  }
  
  ll(unit = "MB")
  
  timestamp(prefix = "## reading file ")
  dt <- fread(tidyDataFile, stringsAsFactors = FALSE, encoding = "UTF-8") %>% 
    as.tbl()
  
  timestamp(prefix = paste0("## combining tables "))
  dt <- dt %>% mutate(key = X)
  
  dt <- left_join(dt, sectionList[[1]]$dt, by = "key")
  dt <- left_join(dt, sectionList[[2]]$dt, by = "key")
  dt <- left_join(dt, sectionList[[3]]$dt, by = "key")
  
  sectionList[[1]]$dt <- ""
  sectionList[[2]]$dt <- ""
  sectionList[[3]]$dt <- ""
  
  dt <- dt %>% select(-V1, -X, -urlKey, -metaDescription, -plaintext, -stemTitle, -stemMetaDescription, -stemPlaintext, - key)
  
  write.csv(dt, file.path(stemedArticlesFolder, "stemed_articles_data.csv"), fileEncoding = "UTF-8")
  
  dt <- ""
  dfM <- ""
  
  ll(unit = "MB")
  
  timestamp(prefix = paste0("## END "))
}