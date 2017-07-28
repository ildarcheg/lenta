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
  res1 <- res
  
  res <- res1
  system.time(res <- gsub("[{}]", "", res))
  system.time(res <- gsub("(\\|[^ ]+)", "", res))
  system.time(res <- gsub("\\?|,", "", res))
  system.time(res <- gsub("\\s+", " ", res))
  system.time(res <- unlist(strsplit(res, split = "\\\\n")))
  system.time(res <- gsub("(\\\\[^ ]+)", "", res))
  system.time(res <- gsub("_", "", res))
  system.time(res <- res[res!=""])
  system.time(sep <- which(grepl("httpslentaru|httplentaru", res)))
  sep <- c(sep, length(res)+1)
  
  print(Sys.time())
  df <- data.frame(urlKey = character(), type = character(), content = character(), stringsAsFactors = FALSE)
  for(i in 1:(length(sep)-1)){
    #print(sep[i])
    #print(paste0(sep[i]+1, ":", sep[i+1]-1))
    df[i, "urlKey"] <- sep[i]
    df[i, "type"] <- "stemedTitles"
    df[i, "content"] <- paste0(res[(sep[i]+1):(sep[i+1]-1)], collapse = " ")
  }
  print(Sys.time())
}