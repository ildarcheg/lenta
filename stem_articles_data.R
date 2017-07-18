require(data.table)
require(stringr)
require(tm)
require(dplyr)
require(tidyr)

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
  dfM <- dfM[, -c("V1")]
  dt <- dfM %>% as.tbl() %>% select(urlKey, title, metaDescription, plaintext)  

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

  write.csv(dt, file.path(dataFolder, "tidy_articles_data_ready_for_stem.csv"), fileEncoding = "UTF-8") 
  dt <- dt %>% select(urlKey, stemTitle, stemMetaDescription, stemPlaintext)
  write.csv(dt, file.path(dataFolder, "tidy_articles_data_stem.csv"), fileEncoding = "UTF-8")


  dfM <- fread(file.path(dataFolder, "tidy_articles_data_stem.csv"), stringsAsFactors = FALSE, encoding = "UTF-8")
  dfM <- dfM[, -c("V1")]
  
  dt <- dfM %>% as.tbl()

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


   dt$url[1]

}