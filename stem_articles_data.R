require(data.table)
require(stringr)
require(tm)
require(dplyr)
require(tidyr)
require(stringr)

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
  stopWords <- readLines(file.path(dataFolder, "stop_words.csv"), warn = FALSE, encoding = "UTF-8")
  dfM <- fread(file.path(dataFolder, "tidy_articles_data.csv"), stringsAsFactors = FALSE, encoding = "UTF-8")
  
  dt1 <- dfM %>% select(url, title, metaDescription, plaintext) %>% as.tbl() 
  
  dt <- dt1
  
  system.time(dt <- dt %>%  mutate(stemTitle = tolower(title), 
           stemMetaDescription = tolower(metaDescription), 
           stemPlaintext = tolower(plaintext))  %>%
    mutate(stemTitle = enc2utf8(stemTitle), 
           stemMetaDescription = enc2utf8(stemMetaDescription), 
           stemPlaintext = enc2utf8(stemPlaintext))  %>% 
    mutate(stemTitle = removeWords(stemTitle, stopWords), 
           stemMetaDescription = removeWords(stemMetaDescription, stopWords), 
           stemPlaintext = removeWords(stemPlaintext, stopWords))  %>% 
    mutate(stemTitle = removePunctuation(stemTitle), 
           stemMetaDescription = removePunctuation(stemMetaDescription), 
           stemPlaintext = removePunctuation(stemPlaintext))  %>%    
    mutate(stemTitle = str_replace_all(stemTitle, "\\s+", " "), 
           stemMetaDescription = str_replace_all(stemMetaDescription, "\\s+", " "), 
           stemPlaintext = str_replace_all(stemPlaintext, "\\s+", " "))  %>%     
    mutate(stemTitle = str_trim(stemTitle, side = "both"), 
           stemMetaDescription = str_trim(stemMetaDescription, side = "both"), 
           stemPlaintext = str_trim(stemPlaintext, side = "both")))
  
  
  write.csv(dt[1:10, ], file.path(dataFolder, "sample_articles_data.csv"), fileEncoding = "UTF-8")
}