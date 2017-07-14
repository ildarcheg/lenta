require(lubridate)
require(dplyr)
require(tidyr)
require(data.table)
require(tldextract)
require(urltools)
require(XML)

Sys.setlocale("LC_ALL", "ru_RU.UTF-8")

# setting working directory for mac and win
if (Sys.getenv("HOMEPATH") == "") {
  workingDirectory <- ("~/lenta")
} else {
  workingDirectory <- paste0(Sys.getenv("HOMEPATH"), "\\lenta")  
}
setwd(workingDirectory)

TityData <- function() {
  dataFolder <- file.path(getwd(), "data")
  dfM <- fread(file.path(dataFolder, "untidy_articles_data.csv"), stringsAsFactors = FALSE, encoding = "UTF-8")
  
  dtD <- dfM %>% 
    select(-V1,-X)  %>% 
    distinct(url, .keep_all=TRUE) %>% 
    na.omit(cols="url")
  
  splitChapters <- function(x) {
    splitOne <- strsplit(x, "lenta.ru:_")[[1]]
    splitLeft <- strsplit(splitOne[1], ",")[[1]]
    splitLeft <- unlist(strsplit(splitLeft, ":_"))
    splitRight <- strsplit(splitOne[2], ":_")[[1]]
    splitRight <- splitRight[splitRight %in% splitLeft]
    splitRight <- gsub("_", " ", splitRight)
    paste0(splitRight, collapse = "|")
  }
  
  dtD <- dtD %>% 
    mutate(chapters = gsub('\"|\\[|\\]| |chapters:', "", chapters)) %>%
    select(-rubric) %>%
    mutate(chaptersFormatted = as.character(sapply(chapters, splitChapters))) %>%
    separate(col = "chaptersFormatted", into = c("rubric", "subrubric")
             , sep = "\\|", extra = "drop", fill = "right", remove = FALSE) %>%
    filter(!rubric == "NA") %>%
    select(-chapters, -chaptersFormatted) 
  
  pattern <- 'Фото: |Фото |Кадр: |Изображение: |, архив|(архив)|©|«|»|\\(|)|\"'
  dtD <- dtD %>% 
    mutate(imageCredits = gsub(pattern, "", imageCredits)) %>%
    separate(col = "imageCredits", into = c("imageCreditsPerson", "imageCreditsCompany")
             , sep = "/", extra = "drop", fill = "left", remove = FALSE) %>%
    mutate(imageCreditsPerson = as.character(sapply(imageCreditsPerson, trimws))) %>%
    mutate(imageCreditsCompany = as.character(sapply(imageCreditsCompany, trimws))) %>%
    select(-imageCredits)
  
  months <- c("января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября", "октября", "ноября", "декабря")
  updateDatetime <- function (datetime, datetimeString, url) {
    #print(datetimeString)
    datetimeNew <- datetime
    if (is.na(datetime)) { 
      if (is.na(datetimeString)) {
        parsedURL <- strsplit(url, "/")[[1]]
        parsedURLLength <- length(parsedURL)
        d <- parsedURL[parsedURLLength-1]
        m <- parsedURL[parsedURLLength-2]
        y <- parsedURL[parsedURLLength-3] 
        H <- round(runif(1, 8, 21))
        M <- round(runif(1, 1, 59))
        S <- 0
        datetimeString <- paste0(paste0(c(y, m, d), collapse = "-"), " ", paste0(c(H, M, S), collapse = ":"))
        datetimeNew <- ymd_hms(datetimeString, tz = "Europe/Moscow", quiet = TRUE)
      } else {
        parsedDatetimeString <- unlist(strsplit(datetimeString, ",")) %>% trimws %>% strsplit(" ") %>% unlist()
        monthNumber <- which(grepl(parsedDatetimeString[3], months))
        datetimeString <- paste0(paste0(c(parsedDatetimeString[4], monthNumber, parsedDatetimeString[2]), collapse = "-"), " ", parsedDatetimeString[1], ":00")
        datetimeNew <- ymd_hms(datetimeString, tz = "Europe/Moscow", quiet = TRUE)
      }
    }  
    #print(datetimeNew)
    datetimeNew
  }
  
  dtD <- dtD %>% 
    mutate(datetime = ymd_hms(datetime, tz = "Europe/Moscow", quiet = TRUE)) %>% 
    mutate(datetimeNew = mapply(updateDatetime, datetime, datetimeString, url)) %>%
    mutate(datetime = as.POSIXct(datetimeNew, tz = "Europe/Moscow",origin = "1970-01-01"))

  dtD <- dtD %>%
    as.data.table() %>%
    na.omit(cols="datetime") %>%
    select(-filename, -title, -metaType, -datetimeString, -datetimeNew) %>%
    rename(title = metaTitle) %>%
    select(url, datetime, rubric, subrubric, title, metaDescription, plaintext, 
           authorLinks, additionalLinks, plaintextLinks, imageDescription, imageCreditsPerson,
           imageCreditsCompany, videoDescription, videoCredits)
    
     
  updateAdditionalLinks <- function(additionalLinks) {
    if (is.na(additionalLinks)) {
      return(NA)
    }
    log <- FALSE
    if (log == TRUE) {
      print(url)
      print("---")
      print(additionalLinks)
    }
    additionalLinksSplitted <- unlist(strsplit(additionalLinks, " "))
    additionalLinksSplitted <- additionalLinksSplitted[!grepl("lenta.ru", additionalLinksSplitted)]
    additionalLinksSplitted <- unlist(strsplit(additionalLinksSplitted, "/[^/]*$"))
    additionalLinksSplitted <- gsub("href=|-–-|«|»|…|,", "", additionalLinksSplitted)
    additionalLinksSplitted <- gsub("[а-я|А-Я]", "eng", additionalLinksSplitted)
    #additionalLinksSplitted <- gsub("-–-|\\[|\\]|’|html.|href=|\\|", "", additionalLinksSplitted)
    additionalLinksSplitted <- gsub("http://http://|https://https://", "http://", additionalLinksSplitted)
    additionalLinksSplitted <- additionalLinksSplitted[grepl("http:|https:", additionalLinksSplitted)]
    
    if (!length(additionalLinksSplitted) == 0) {
      URLSplitted <- tryCatch(sapply(additionalLinksSplitted, parseURI), error = function(x) {return(NA)})
      if (is.na(URLSplitted[1])) {
        print("------")
        print(additionalLinks)
        print(additionalLinksSplitted)
        return(NA)
      }
      URLSplitted <- unlist(URLSplitted["server",])
      domain <- paste0(tldextract(URLSplitted)$domain, ".", 
                       tldextract(URLSplitted)$tld)
      paste0(domain, collapse = " ")
    } else {
      NA
    }
  }
  
  dtD1 <- as.tbl(dtD)
  
  print(Sys.time())
  system.time(dt1 <- dtD1[100001:200000, c("additionalLinks")] %>%
    mutate(additionalLinks = mapply(updateAdditionalLinks, additionalLinks)))
  print(Sys.time())
  system.time(dt1 <- dtD1[200001:300000, c("additionalLinks")] %>%
    mutate(additionalLinks = mapply(updateAdditionalLinks, additionalLinks)))
  print(Sys.time())
  system.time(dt1 <- dtD1[300001:400000, c("additionalLinks")] %>%
    mutate(additionalLinks = mapply(updateAdditionalLinks, additionalLinks)))

}
