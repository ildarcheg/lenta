require(lubridate)
require(dplyr)
require(tidyr)
require(data.table)
require(tldextract)
require(XML)

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
baseURL <- "https://lenta.ru"
parsedArticlesFolder <- file.path(getwd(), "parsed_articles")


## STEP 5. Clear and tidy data
# 
TityData <- function() {
  
  dfM <- fread(file.path(parsedArticlesFolder, "untidy_articles_data.csv"), 
               stringsAsFactors = FALSE, encoding = "UTF-8")
  
  # Remove duplicate rows, remove rows with url = NA, create urlKey column as a key
  dtD <- dfM %>% 
    select(-V1,-X)  %>% 
    distinct(url, .keep_all=TRUE) %>% 
    na.omit(cols="url") %>%
    mutate(urlKey = gsub(":|\\.|/", "", url))
  
  # Function SplitChapters is used to process formatted chapter column and retrive rubric 
  # and subrubric  
  SplitChapters <- function(x) {
    splitOne <- strsplit(x, "lenta.ru:_")[[1]]
    splitLeft <- strsplit(splitOne[1], ",")[[1]]
    splitLeft <- unlist(strsplit(splitLeft, ":_"))
    splitRight <- strsplit(splitOne[2], ":_")[[1]]
    splitRight <- splitRight[splitRight %in% splitLeft]
    splitRight <- gsub("_", " ", splitRight)
    paste0(splitRight, collapse = "|")
  }
  
  # Process chapter column to retrive rubric and subrubric
  # Column value such as:
  # chapters: ["Бывший_СССР","Украина","lenta.ru:_Бывший_СССР:_Украина:_Правительство_ФРГ_сочло_неприемлемым_создание_Малороссии"], // Chapters страницы
  # should be represented as rubric value "Бывший СССР" 
  # and subrubric value "Украина"
  dtD <- dtD %>% 
    mutate(chapters = gsub('\"|\\[|\\]| |chapters:', "", chapters)) %>%
    select(-rubric) %>%
    mutate(chaptersFormatted = as.character(sapply(chapters, SplitChapters))) %>%
    separate(col = "chaptersFormatted", into = c("rubric", "subrubric")
             , sep = "\\|", extra = "drop", fill = "right", remove = FALSE) %>%
    filter(!rubric == "NA") %>%
    select(-chapters, -chaptersFormatted) 
  
  # Process imageCredits column and split into imageCreditsPerson 
  # and imageCreditsCompany
  # Column value such as: "Фото: Игорь Маслов / РИА Новости" should be represented
  # as imageCreditsPerson value "Игорь Маслов" and 
  # imageCreditsCompany value "РИА Новости"
  pattern <- 'Фото: |Фото |Кадр: |Изображение: |, архив|(архив)|©|«|»|\\(|)|\"'
  dtD <- dtD %>% 
    mutate(imageCredits = gsub(pattern, "", imageCredits)) %>%
    separate(col = "imageCredits", into = c("imageCreditsPerson", "imageCreditsCompany")
             , sep = "/", extra = "drop", fill = "left", remove = FALSE) %>%
    mutate(imageCreditsPerson = as.character(sapply(imageCreditsPerson, trimws))) %>%
    mutate(imageCreditsCompany = as.character(sapply(imageCreditsCompany, trimws))) %>%
    select(-imageCredits)
  
  # Function UpdateDatetime is used to process missed values in datetime column
  # and fill them up with date and time retrived from string presentation 
  # such as "13:47, 18 июля 2017" or from url such 
  # as https://lenta.ru/news/2017/07/18/frg/. Hours and Minutes set randomly
  # from 8 to 21 in last case
  months <- c("января", "февраля", "марта", "апреля", "мая", "июня", "июля", 
              "августа", "сентября", "октября", "ноября", "декабря")
  UpdateDatetime <- function (datetime, datetimeString, url) {
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
        datetimeString <- paste0(paste0(c(y, m, d), collapse = "-"), " ", 
                                 paste0(c(H, M, S), collapse = ":"))
        datetimeNew <- ymd_hms(datetimeString, tz = "Europe/Moscow", quiet = TRUE)
      } else {
        parsedDatetimeString <- unlist(strsplit(datetimeString, ",")) %>% 
          trimws %>% 
          strsplit(" ") %>% 
          unlist()
        monthNumber <- which(grepl(parsedDatetimeString[3], months))
        dateString <- paste0(c(parsedDatetimeString[4], monthNumber, 
                               parsedDatetimeString[2]), collapse = "-")
        datetimeString <- paste0(dateString, " ", parsedDatetimeString[1], ":00")
        datetimeNew <- ymd_hms(datetimeString, tz = "Europe/Moscow", quiet = TRUE)
      }
    }  
    datetimeNew
  }
  
  # Process datetime and fill up missed values
  dtD <- dtD %>% 
    mutate(datetime = ymd_hms(datetime, tz = "Europe/Moscow", quiet = TRUE)) %>% 
    mutate(datetimeNew = mapply(UpdateDatetime, datetime, datetimeString, url)) %>%
    mutate(datetime = as.POSIXct(datetimeNew, tz = "Europe/Moscow",origin = "1970-01-01"))
  
  # Remove rows with missed datetime values, replace title with metaTitle,
  # remove columns that we do not need anymore  
  dtD <- dtD %>%
    as.data.table() %>%
    na.omit(cols="datetime") %>%
    select(-filename, -title, -metaType, -datetimeString, -datetimeNew) %>%
    rename(title = metaTitle) %>%
    select(url, urlKey, datetime, rubric, subrubric, title, metaDescription, plaintext, 
           authorLinks, additionalLinks, plaintextLinks, imageDescription, imageCreditsPerson,
           imageCreditsCompany, videoDescription, videoCredits)
  
  # Function UpdateAdditionalLinks is used to process missed values in datetime column
  # and fill them up with date and time retrived from string presentation 
  # such as "13:47, 18 июля 2017" or from url such 
  # as https://lenta.ru/news/2017/07/18/frg/. Hours and Minutes set randomly
  # from 8 to 21 in last case  
  UpdateAdditionalLinks <- function(additionalLinks, url) {
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
    } else {
      NA
    }
  }
  
  UpdateAdditionalLinksDomain <- function(additionalLinks, url) {
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
    mutate(additionalLinks = gsub(symbolsHttp2, "http://", additionalLinks))
  
  dtD <- dtD %>% 
    mutate(plaintextLinks = mapply(UpdateAdditionalLinks, plaintextLinks, url)) %>%
    mutate(additionalLinks = mapply(UpdateAdditionalLinks, additionalLinks, url))
  
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
