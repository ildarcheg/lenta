require(lubridate)
require(rvest)
require(dplyr)
require(purrr)
require(XML)

baseURL <- "https://lenta.ru"

# Sys.setlocale("LC_ALL", "ru_RU.UTF-8")

# setting working directory for mac and win
if (Sys.getenv("HOMEPATH") == "") {
  workingDirectory <- ("~/lenta")
} else {
  workingDirectory <- paste0(Sys.getenv("HOMEPATH"), "\\lenta")  
}
setwd(workingDirectory)

## SERVICE FUNCTION
SetNAIfZeroLength <- function(param) {
  param <- param[!is.na(param)]
  paramLength <- length(param)
  if (paramLength==0) {param <- NA}
  return(param)
}

## STEP 1 CODE
# donloading list of pages with archived articles
GetNewsListForPeriod <- function(startDate, endDate) {
  dayArray <- seq(as.Date(startDate), as.Date(endDate), by="days")
  archiveLinkList <- paste0(baseURL, "/", year(dayArray), "/", 
                            formatC(month(dayArray), width = 2, format = "d", 
                                    flag = "0"), "/", formatC(day(dayArray), 
                                                              width = 2, format = "d", flag = "0"), "/")
  newsList <- c()
  for (i in 1:length(archiveLinkList)) {
    print(archiveLinkList[i])
    print(i)
    pg <- read_html(archiveLinkList[i], encoding = "UTF-8")
    total <- html_nodes(pg, xpath=".//section[@class='b-longgrid-column']//div[@class='titles']//a") %>% html_attr("href")   
    newsList <- c(newsList, total)
    
    saveRDS(newsList, file = "data/tempNewsList.rds")
  }
  return(newsList)
}

# getting links to all articles for 2010-2017
Step1 <- function() {
  newsList <- GetNewsListForPeriod(as.Date("2010-01-01"), as.Date("2017-06-30"))
  newsLinkList <- paste0(baseURL, newsList)
  saveRDS(newsLinkList, file = "data/tempNewsLinkList.rds") 
}

## STEP 2 CODE
# Prepare wget code for pages downloading
GetWgetFiles <- function() {
  newsLinkList <- readRDS("data/tempNewsLinkList.rds") 
  numberOfLinks <- length(newsLinkList)
  digitNumber <- nchar(numberOfLinks)
  groupSize <- 20000
  filesGroup <- seq(from = 1, to = numberOfLinks, by = groupSize)
  for (i in 1:length(filesGroup)) {
    firstFileInGroup <- filesGroup[i]
    lastFileInGroup <- min(firstFileInGroup + groupSize - 1, numberOfLinks)
    leftPartFolderName <- formatC(firstFileInGroup, width = digitNumber, format = "d", flag = "0")
    rigthPartFolderName <- formatC(lastFileInGroup, width = digitNumber, format = "d", flag = "0")
    subFolderName <- paste0(leftPartFolderName, "-", rigthPartFolderName)
    subFolderPath <- file.path(getwd(), paste0("data/",subFolderName))
    dir.create(subFolderPath)
    print(subFolderName)
    writeLines(newsLinkList[firstFileInGroup:lastFileInGroup], file.path(subFolderPath, "list.urls"))
    cmdCode <-"..\\wget -i list.urls"
    writeLines(cmdCode, file.path(subFolderPath, "start.cmd"))
  }
}

# Create folders and cmd files
Step2 <- function () {
  GetWgetFiles()
}

## STEP 3 CODE
# Validation of downloaded files
ValidateDownloadedFiles <- function() {
  files <- list.files(file.path(getwd(), "data"), full.names = TRUE, recursive = TRUE, pattern = "index")
  downloadedLonls <- c()
  for (i in 1:length(files)) {
    currentFile <- files[i]
    pg <- read_html(currentFile, encoding = "UTF-8")
    fileLink <- html_nodes(pg, xpath=".//link[@rel='canonical']") %>% html_attr("href")   
    downloadedLonls <- c(downloadedLonls, fileLink)  
    saveRDS(downloadedLonls, file = "data/tempDownloadedList.rds")
  }
}

## STEP 4 CODE
# Validation of downloaded files
ReadFile <- function(filename) {
  
  pg <- read_html(filename, encoding = "UTF-8")
  
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
  
  data.frame(url = url, 
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
  
}

ReadFilesInFolder <- function(folderNumber) {
  dataFolder <- file.path(getwd(), "data")
  folders <- list.files(dataFolder, full.names = FALSE, recursive = FALSE, pattern = "-")
  folderName <- folders[folderNumber]
  currentFolder <- file.path(dataFolder, folderName)
  files <- list.files(currentFolder, full.names = TRUE, recursive = FALSE, pattern = "index")
  
  numberOfFiles <- length(files)
  print(numberOfFiles)
  groupSize <- 1000
  filesGroup <- seq(from = 1, to = numberOfFiles, by = groupSize)
  dfList <- list()
  for (i in 1:length(filesGroup)) {
    firstFileInGroup <- filesGroup[i]
    lastFileInGroup <- min(firstFileInGroup + groupSize - 1, numberOfFiles)
    print(paste0(firstFileInGroup, "-", lastFileInGroup))
    dfList[[i]] <- map_df(files[firstFileInGroup:lastFileInGroup], ReadFile)
  }
  df <- bind_rows(dfList)
  write.csv(df, file.path(dataFolder, paste0("dfs/", folderName, ".csv")))
}

#Step1()
#Step2()
#Step3()


