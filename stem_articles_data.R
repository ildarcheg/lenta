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

# Load library that helps to chunk vectors
source("chunk.R")

# Set common variables
tidyArticlesFolder <- file.path(getwd(), "tidy_articles")
stemedArticlesFolder <- file.path(getwd(), "stemed_articles")

# Create required folders if not exist 
dir.create(stemedArticlesFolder, showWarnings = FALSE)

## STEP 6. Stem title, description and plain text
# Write columns on disk, run mystem, read stemed data and add to data.table
StemArticlesData <- function() {
  
  # Read tidy data and keep only column that have to be stemed.
  # Add === separate rows in stem output.
  # dt that takes about 5GB RAM for 700000 obs. of 25 variables
  # and 2.2GB for 700000 obs. of 5 variables as tbl
  timestamp(prefix = "## START reading file ")
  tidyDataFile <- file.path(tidyArticlesFolder, "tidy_articles_data.csv")
  dt <- fread(tidyDataFile, stringsAsFactors = FALSE, encoding = "UTF-8") %>% 
    as.tbl()
  dt <- dt %>% mutate(sep = "===") %>%
    select(sep, X, stemTitle, stemMetaDescription, stemPlaintext)
  
  # Check memory usage 
  print(ll(unit = "MB"))
  
  # Prepare the list that helps us to stem 3 column 
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
  
  timestamp(prefix = "## steming file ")
  # Write the table with sep, X, columnToStem columns and run mystem.
  # It takes about 30 min to process Title, MetaDescription and Plaintext
  # in 700K rows table.
  # https://tech.yandex.ru/mystem/
  for (i in 1:length(sectionList)) {
    write.table(dt[, c("sep","X", sectionList[[i]]$columnToStem)], 
                sectionList[[i]]$sourceFile, 
                fileEncoding = "UTF-8", sep = ",", quote = FALSE, 
                row.names = FALSE, col.names = FALSE)
    system(paste0("mystem -nlc ", sectionList[[i]]$sourceFile, " ", 
                  sectionList[[i]]$stemedFile), intern = FALSE)  
  }
  
  # Remove dt from memory and call garbage collection
  rm(dt)
  gc()
  
  # Check memory usage 
  print(ll(unit = "MB"))
  
  timestamp(prefix = "## process file ")
  # Process stemed files. it takes about 60 min to process 3 stemed files
  for (i in 1:length(sectionList)) {
    stemedText <- readLines(sectionList[[i]]$stemedFile, 
                            warn = FALSE, 
                            encoding = "UTF-8")
    
    # Split stemed text in chunks
    chunkList <- chunk(stemedText, chunk.size = 10000000)
    
    # Clean chunks one by one and remove characters that were added by mystem
    resLines <- c()
    for (j in 1:length(chunkList)) {
      resTemp <- chunkList[[j]] %>% 
        str_replace_all("===,", "===") %>%
        strsplit(split = "\\\\n|,") %>% unlist() %>% 
        str_replace_all("(\\|[^ ]+)|(\\\\[^ ]+)|\\?|,|_", "")
      resLines <- c(resLines, resTemp[resTemp!=""])
    }  
    
    # Split processed text in rows using === added at the beginnig  
    chunkedRes <- chunk(resLines, chunk.delimiter = "===", 
                        fixed.delimiter = FALSE, 
                        keep.delimiter = TRUE)
    
    # Process each row and extract key (row number) and stemed content
    stemedList <- lapply(chunkedRes, 
                         function(x) {
                           data.frame(key = as.integer(str_replace_all(x[1], "===", "")), 
                                      content = paste0(x[2:length(x)], collapse = " "), 
                                      stringsAsFactors = FALSE)})
    
    # Combine all rows in data frame with key and content colums
    sectionList[[i]]$dt <- bind_rows(stemedList)
    colnames(sectionList[[i]]$dt) <- c("key", sectionList[[i]]$stemedColumn)
    
  }
  
  # Remove variables used in loop and call garbage collection
  rm(stemedText, chunkList, resLines, chunkedRes, stemedList)
  gc()
  
  # Check memory usage 
  print(ll(unit = "MB"))
  
  # read tidy data again
  timestamp(prefix = "## reading file (again)")
  dt <- fread(tidyDataFile, stringsAsFactors = FALSE, encoding = "UTF-8") %>% 
    as.tbl()
  
  # add key column as a key and add tables with stemed data to tidy data 
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
  
  file.remove(sectionList[[1]]$sourceFile)
  file.remove(sectionList[[2]]$sourceFile)
  file.remove(sectionList[[3]]$sourceFile)
  file.remove(sectionList[[1]]$stemedFile)
  file.remove(sectionList[[2]]$stemedFile)
  file.remove(sectionList[[3]]$stemedFile)
  
  # Remove dt, sectionList and call garbage collection
  rm(dt)
  gc()
  
  # Check memory usage
  print(ll(unit = "MB"))
  
  timestamp(prefix = "## END ")
}