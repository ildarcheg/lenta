require(lubridate)
require(rvest)

baseURL <- "https://lenta.ru"

# setting working directory for mac and win
if (Sys.getenv("HOMEPATH") == "") {
  workingDirectory <- ("~/lenta")
} else {
  workingDirectory <- paste0(Sys.getenv("HOMEPATH"), "\\lenta")  
}
setwd(workingDirectory)


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
ParseDownloadedFiles <- function() {
  files <- list.files(file.path(getwd(), "data"), full.names = TRUE, recursive = TRUE, pattern = "index")
  downloadedLonls <- c()
  for (i in 1:length(files)) {
    currentFile <- files[i]
    #print(i)
    #print(currentFile)
    pg <- read_html(currentFile, encoding = "UTF-8")
    fileLink <- html_nodes(pg, xpath=".//link[@rel='canonical']") %>% html_attr("href")   
    downloadedLonls <- c(downloadedLonls, fileLink)  
    #saveRDS(downloadedLonls, file = "data/tempDownloadedList.rds")
  }
  return(downloadedLonls)
}
#Step1()
#Step2()
#Step3()


