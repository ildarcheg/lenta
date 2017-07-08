library(stringi)
library(purrr)
library(rvest)
library(dplyr)

SetNAIfZeroLength <- function(param) {
  param <- param[!is.na(param)]
  paramLength <- length(param)
  if (paramLength==0) {param <- NA}
  return(param)
}

ParseHtmlText <- function(dataToParse) {
  
  urlMain <- dataToParse$url 
  print(urlMain)
  print(dataToParse)
  htmlLines <- dataToParse$htmlLines
  
  pg <- read_html(htmlLines, encoding = "UTF-8")
  
  title <- html_text(html_nodes(pg, xpath=".//head/title"))
  title <- SetNAIfZeroLength(title)
  
  articleBodyNode <- html_nodes(pg, xpath=".//div[@itemprop='articleBody']")
  
  plaintext <- html_nodes(articleBodyNode, xpath=".//p") %>% html_text() %>% paste0(collapse="") 
  if (plaintext == "") {
    plaintext <- NA
  }
  plaintextLinks <- html_nodes(articleBodyNode, xpath=".//a") %>% html_attr("href") %>% unique() %>% paste0(collapse=" ")
  if (plaintextLinks == "") {
    plaintextLinks <- NA
  }
  
  additionalLinks <- html_nodes(pg, xpath=".//section/div[@class='item']/div/..//a") %>% html_attr("href") %>% unique() %>% paste0(collapse=" ")
  if (additionalLinks == "") {
    additionalLinks <- NA
  }
  
  imageNodes <- html_nodes(pg, xpath=".//div[@class='b-topic__title-image']")
  imageDescription <- html_nodes(imageNodes, xpath="div/div[@class='b-label__caption']") %>% html_text() %>% unique() 
  imageDescription <- SetNAIfZeroLength(imageDescription)
  imageCredits <- html_nodes(imageNodes, xpath="div/div[@class='b-label__credits']") %>% html_text() %>% unique() 
  imageCredits <- SetNAIfZeroLength(imageCredits)
  
  if (is.na(imageDescription)&is.na(imageCredits)) {
    videoNodes <- html_nodes(pg, xpath=".//div[@class='b-video-box__info']")
    videoDescription <- html_nodes(videoNodes, xpath="div[@class='b-video-box__caption']") %>% html_text() %>% unique() 
    videoDescription <- SetNAIfZeroLength(videoDescription)
    videoCredits <- html_nodes(videoNodes, xpath="div[@class='b-video-box__credits']") %>% html_text() %>% unique() 
    videoCredits <- SetNAIfZeroLength(videoCredits)
  } else {
    videoDescription <- NA
    videoCredits <- NA
  }
  
  url <- html_attr(html_nodes(pg, xpath=".//head/link[@rel='canonical']"), "href")
  url <- SetNAIfZeroLength(url)
  
  authorSection <- html_nodes(pg, xpath=".//p[@class='b-topic__content__author']")
  authors <- html_nodes(authorSection, xpath="//span[@class='name']") %>% html_text()
  authors <- SetNAIfZeroLength(authors)
  if (length(authors) > 1) {
    authors <- paste0(authors, collapse = ",")
  }
  authorLinks <- html_nodes(authorSection, xpath="a") %>% html_attr("href")
  authorLinks <- SetNAIfZeroLength(authorLinks)
  if (length(authorLinks) > 1) {
    authorLinks <- paste0(authorLinks, collapse = ",")
  }
  
  #itemprop="datePublished"
  #.//time[@class='g-date']
  datetimeString <- html_nodes(pg, xpath=".//div[@class='b-topic__info']/time[@class='g-date']") %>% html_text() %>% unique()
  datetimeString <- SetNAIfZeroLength(datetimeString)
  
  datetime <- html_nodes(pg, xpath=".//div[@class='b-topic__info']/time[@class='g-date']") %>% html_attr("datetime") %>% unique()
  datetime <- SetNAIfZeroLength(datetime)
  
  # print(paste("Part ", filename))
  # print("-")
  # print(url)
  # print("-")
  # print(datetime)
  # print("-")
  # print(datetimeString)
  # print("-")
  # print(title)
  # print("-")
  # print(authors)
  # print("-")
  # print(authorLinks)
  # print("-")
  # print(additionalLinks)
  # print("-")
  # print(imageDescription)
  # print("-")
  # print(imageCredits)
  # print("-")
  # print(videoDescription)
  # print("-")
  # print(videoCredits)
  # print("----")
  
  ##ВНИМАНИЕ ДОБАВИТЬ РАЗДЕЛЫ
  ##chapters: ["Наука_и_техника","Космос","lenta.ru:_Наука_и_техника:_Космос:_Роскосмос_увидел_огромного_Путина_с_орбиты"], // Chapters страницы
  data.frame(urlMain = urlMain,
             url = url, 
             datetime = datetime,
             datetimeString = datetimeString,
             title = title, 
             plaintext = plaintext, 
             authors = authors, 
             authorLinks = authorLinks,
             additionalLinks = additionalLinks, 
             imageDescription,
             imageCredits,
             videoDescription,
             videoCredits,
             stringsAsFactors=FALSE)  
  data.frame(url=url, htmlLines = htmlLines, stringsAsFactors=FALSE)
}

ReadWarcFileNewByLines <- function(warc_file) {
  archive <- file(warc_file, open="r")
  continue <- TRUE
  listOfData <- list(l1=NULL)
  l1 <- list()
  n <- 0
  htmlLines <- c()
  isHtmlLines = FALSE
  while (continue == TRUE) {
    line <- readLines(archive, n=1, encoding = "UTF-8", warn=TRUE) 
    if (length(line) == 0) {
      continue == FALSE
      break()
    }
    
    if (line == "WARC-Type: request") {
      if (!length(l1) == 0) {
        n <- n + 1
        listOfData[[n]] <- l1
      }
      l1 <- list(url=NULL, htmlLines=NULL)
    }
    if (grepl("WARC-Target-URI:", line)) {
      l1$url <- gsub(pattern = "[<>]", replacement = "", x = stri_split_fixed(line, " ", 2)[[1]][2])
    }
    if (grepl("<!DOCTYPE html>", line)) {
      isHtmlLines = TRUE
      htmlLines <- c()
    }  
    if (isHtmlLines) {
      htmlLines <- c(htmlLines, line)
    }    
    if (grepl("</html>", line)) {
      isHtmlLines = FALSE
      l1$htmlLines <- paste0(htmlLines, collapse = " ")
    } 
    
  }
  close(archive)
  listOfData
  
}


ReadWarcFileNew <- function(warc_file) {
  archive <- file(warc_file, open="r")
  lines <- readLines(archive, warn=TRUE) 
  close(archive)
}

#' get the number of records in a warc request
warc_request_record_count <- function(warc_fle) {
  archive <- file(warc_fle, open="r")
  rec_count <- 0
  while (length(line <- readLines(archive, n=1, warn=FALSE)) > 0) {
    if (grepl("^WARC-Type: request", line)) {
      rec_count <- rec_count + 1
    }
  }
  close(archive)
  rec_count
}

warc_response_index <- function(warc_file, warcFileName) {
  
  record_count=warc_request_record_count(warc_file)
  
  records <- vector("list", record_count)
  archive <- file(warc_file, open="r")

  idx <- 0
  record <- list(url=NULL, pos=NULL, length=NULL, warc=NULL, warcFileName = NULL)
  in_request <- FALSE
  
  while (length(line <- readLines(archive, n=1, warn=TRUE)) > 0) {
    if (grepl("^WARC-Type:", line)) {
      if (grepl("response", line)) {
        if (idx > 0) {
          records[[idx]] <- record
          record <- list(url=NULL, pos=NULL, length=NULL, warc=NULL)
        }
        in_request <- TRUE
        idx <- idx + 1
      } else {
        in_request <- FALSE
      }
    }
    
    if (in_request & grepl("^WARC-Target-URI:", line)) {
      record$url <- stri_match_first_regex(line, "^WARC-Target-URI: (.*)")[,2]
      record$warc <- warc_file
      record$warcFileName <- warcFileName
    }
    
    if (in_request & grepl("^Content-Length:", line)) {
      record$length <- as.numeric(stri_match_first_regex(line, "Content-Length: ([[:digit:]]+)")[,2])
      record$pos <- as.numeric(seek(archive, NA))
    }
  }
  
  close(archive)
  records[[idx]] <- record
  records
}

get_warc_response <- function(warc_file, pos, length) {
  archive <- file(warc_file, open="r")
  seek(archive, pos)
  record <- readChar(archive, length)
  record <- stri_split_fixed(record, "\r\n\r\n", 2)[[1]]
  names(record) <- c("header", "page")
  close(archive)

  as.list(record)
}

GetDF <- function(r) {
  resp <- get_warc_response(r$warc, r$pos, r$length)
  pg <- read_html(stri_split_fixed(resp$page, "\r\n", 2)[[1]][2])
  html_nodes(pg, xpath=".//div[@itemprop='articleBody']/..//p") %>%
    html_text() %>%
    paste0(collapse="") -> plantext
  title <- html_text(html_nodes(pg, xpath=".//head/title"))
  
  data.frame(url=r$url, title, plantext, stringsAsFactors=FALSE)
}

ReadWarcFile <- function(responses, warcCSVFolder) {
  
  for (i in 93:length(responses)) {
    print(i)
    df <- GetDF(responses[[i]])  
  }
  df <- GetDF(responses)
  write.csv(df, file.path(warcCSVFolder, paste0(responses$warcFileName,".csv")), fileEncoding = "UTF-8")
}

ReadWarcFiles <- function() {
  dataFolder <- file.path(getwd(), "data")
  warcFolder <- file.path(dataFolder, "warc")
  warcCSVFolder <- file.path(dataFolder, "warcCSV")
  warcFiles <- list.files(warcFolder, full.names = TRUE, recursive = FALSE)
  warcFilesNames <- list.files(warcFolder, full.names = FALSE, recursive = FALSE)
  numberOfWarcFiles <- length(warcFiles)
  groupRecords <- vector("list", numberOfWarcFiles)
  responses <- list()
  
  for (i in 1:numberOfWarcFiles) {
    response <- warc_response_index(warcFiles[i], warcFilesNames[i])
    if (i == 1) {
      responses <- response 
      print("init")
    } else {
      responses <- append(responses, response)
      print("append")
    }
  }
  
}

#warc_file <- file.path(getwd(), "data/lenta.warc")

#responses <- warc_response_index(warc_file)

