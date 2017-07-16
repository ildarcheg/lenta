require(lubridate)
require(dplyr)
require(tidyr)
require(data.table)
require(tldextract)
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
  
  
  updateAdditionalLinks <- function(additionalLinks, url) {
    if (is.na(additionalLinks)) {
      return(NA)
    }
    
    additionalLinksSplitted <- gsub("http://|https://|http:///|https:///"," ", additionalLinks)
    additionalLinksSplitted <- gsub("http:/|https:/|htt://","", additionalLinksSplitted)
    additionalLinksSplitted <- trimws(additionalLinksSplitted)
    additionalLinksSplitted <- unlist(strsplit(additionalLinksSplitted, " "))
    additionalLinksSplitted <- additionalLinksSplitted[!additionalLinksSplitted==""]
    additionalLinksSplitted <- additionalLinksSplitted[!grepl("lenta.", additionalLinksSplitted)]
    additionalLinksSplitted <- unlist(strsplit(additionalLinksSplitted, "/[^/]*$"))
    additionalLinksSplitted <- paste0("http://", additionalLinksSplitted)
    additionalLinksSplitted <- additionalLinksSplitted[grepl("http:|https:", additionalLinksSplitted)]
    
    if (!length(additionalLinksSplitted) == 0) {
      URLSplitted <- c()
      for(i in 1:length(additionalLinksSplitted)) {
        parsed <- tryCatch(parseURI(additionalLinksSplitted[i]), error = function(x) {return(NA)}) 
        parsedURL <- parsed["server"]
        if (!is.na(parsedURL)) {
          URLSplitted <- c(URLSplitted, parsedURL) 
        }
      }
      if (length(URLSplitted)==0){
        NA
      } else {
        URLSplitted <- URLSplitted[!is.na(URLSplitted)]
        paste0(URLSplitted, collapse = " ")
      }
      #domain <- paste0(tldextract(URLSplitted)$domain, ".", 
      #                 tldextract(URLSplitted)$tld)
      #return(NA)
      #paste0(domain, collapse = " ")
    } else {
      NA
    }
  }
  
  updateAdditionalLinksDomain <- function(additionalLinks, url) {
    if (is.na(additionalLinks)|(additionalLinks=="NA")) {
      return(NA)
    }
    additionalLinksSplitted <- unlist(strsplit(additionalLinks, " "))
    if (!length(additionalLinksSplitted) == 0) {
      parsedDomain <- tryCatch(tldextract(additionalLinksSplitted), error = function(x) {data_frame(domain = NA, tld = NA)}) 
      parsedDomain <- parsedDomain[!is.na(parsedDomain$domain), ]
      if (nrow(parsedDomain)==0) {
        #print("--------")
        #print(additionalLinks)
        return(NA)
      }
      domain <- paste0(parsedDomain$domain, ".", parsedDomain$tld)
      domain <- unique(domain)
      domain <- paste0(domain, collapse = " ")
      return(domain)
    } else {
      return(NA)
    }
  }
  
  symbolsToRemove <- "href=|-–-|«|»|…|,|•|“|”|\n|\"|,|[|]|<a|<br" 
  symbolsHttp <- "http:\\\\\\\\|:http://|-http://|.http://"
  symbolsHttp2 <- "http://http://|https://https://"
  symbolsReplace <- "[а-я|А-Я|#!]"
  
  dtD <- dtD %>% 
    mutate(plaintextLinks = gsub(symbolsToRemove,"", plaintextLinks)) %>%
    mutate(plaintextLinks = gsub(symbolsHttp, "http://", plaintextLinks)) %>%
    mutate(plaintextLinks = gsub(symbolsReplace, "e", plaintextLinks)) %>%
    mutate(plaintextLinks = gsub(symbolsHttp2, "http://", plaintextLinks)) %>%
    mutate(additionalLinks = gsub(symbolsToRemove,"", additionalLinks)) %>%
    mutate(additionalLinks = gsub(symbolsHttp, "http://", additionalLinks)) %>%
    mutate(additionalLinks = gsub(symbolsReplace, "e", additionalLinks)) %>%
    mutate(additionalLinks = gsub(symbolsHttp2, "http://", additionalLinks)) %>%
    mutate(plaintextLinks = mapply(updateAdditionalLinks, plaintextLinks, url)) %>%
    mutate(additionalLinks = mapply(updateAdditionalLinks, additionalLinks, url))
  
  numberOfLinks <- nrow(dtD)
  groupSize <- 10000
  groupsN <- seq(from = 1, to = numberOfLinks, by = groupSize)
  
  for (i in 1:length(groupsN)) {
    n1 <- groupsN[i]
    n2 <- min(n1 + groupSize - 1, numberOfLinks) 
    print(paste0(n1, "-", n2))
    dtD$additionalLinks[n1:n2] <- mapply(updateAdditionalLinksDomain, dtD$additionalLinks[n1:n2], dtD$url[n1:n2])
    print(unique(dtD$additionalLinks[n1:n2]))
    dtD$plaintextLinks[n1:n2] <- mapply(updateAdditionalLinksDomain, dtD$plaintextLinks[n1:n2], dtD$url[n1:n2])
    print(unique(dtD$plaintextLinks[n1:n2]))    
  }
  
  write.csv(dtD, file.path(dataFolder, "tidy_articles_data.csv"), fileEncoding = "UTF-8")
  
}
