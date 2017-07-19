# Load required packages
require(lubridate)
require(rvest)
require(dplyr)
require(purrr)
require(XML)
require(data.table)

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
tempDataFolder <- file.path(getwd(), "temp_data")
downloadedArticlesFolder <- file.path(getwd(), "downloaded_articles")
warcFolder <- file.path(downloadedArticlesFolder, "warc")
parsedArticlesFolder <- file.path(getwd(), "parsed_articles")
commandsFolder <- file.path(tempDataFolder, "commands")

articlesStartDate <- as.Date("2010-01-01")
articlesEndDate <- as.Date("2017-06-30")

# Creare required folders if not exist 
dir.create(tempDataFolder, showWarnings = FALSE)
dir.create(downloadedArticlesFolder, showWarnings = FALSE)
dir.create(parsedArticlesFolder, showWarnings = FALSE)
dir.create(commandsFolder, showWarnings = FALSE)
dir.create(warcFolder, showWarnings = FALSE)

## SERVICE FUNCTION
SetNAIfZeroLength <- function(param) {
  param <- param[!is.na(param)]
  paramLength <- length(param)
  if (paramLength==0) {param <- NA}
  return(param)
}

## STEP 1. Prepare articles links list
# Dowload list of pages with archived articles. 
# Takes about 40 minutes
GetNewsListForPeriod <- function() {
  
  # prepare vector of links of archive pages in https://lenta.ru//yyyy/mm/dd/ format
  dayArray <- seq(as.Date(articlesStartDate), as.Date(articlesEndDate), 
                  by="days")
  archivePagesLinks <- paste0(baseURL, "/", year(dayArray), 
                      "/", formatC(month(dayArray), width = 2, format = "d", flag = "0"), 
                      "/", formatC(day(dayArray), width = 2, format = "d", flag = "0"), 
                      "/")
  articlesLinks <- c()
  for (i in 1:length(archivePagesLinks)) {
    pg <- read_html(archivePagesLinks[i], encoding = "UTF-8")
    total <- html_nodes(pg, 
              xpath=".//section[@class='b-longgrid-column']//div[@class='titles']//a") %>% 
              html_attr("href")   
    articlesLinks <- c(articlesLinks, total)
    saveRDS(articlesLinks, file.path(tempDataFolder, "tempArticlesLinks.rds"))
  }
  articlesLinks <- paste0(baseURL, articlesLinks)
  writeLines(articlesLinks, file.path(tempDataFolder, "articles.urls"))
}

## STEP 2. Prepare wget CMD files for parallel downloading
# Create CMD file
# Downloading process takes about 3 hours
CreateWgetCMDFiles <- function() {
  
  articlesLinks <- readLines(file.path(tempDataFolder, "articles.urls"))
  dir.create(warcFolder, showWarnings = FALSE)
  
  # split up articles links array by 10K links 
  numberOfLinks <- length(articlesLinks)
  digitNumber <- nchar(numberOfLinks)
  groupSize <- 10000
  filesGroup <- seq(from = 1, to = numberOfLinks, by = groupSize)
  cmdCodeAll <- c()
  
  for (i in 1:length(filesGroup)) {
    
    # prepare folder name as 00001-10000, 10001-20000 etc
    firstFileInGroup <- filesGroup[i]
    lastFileInGroup <- min(firstFileInGroup + groupSize - 1, numberOfLinks)
    leftPartFolderName <- formatC(firstFileInGroup, width = digitNumber, 
                                  format = "d", flag = "0")
    rigthPartFolderName <- formatC(lastFileInGroup, width = digitNumber, 
                                   format = "d", flag = "0")
    subFolderName <- paste0(leftPartFolderName, "-", rigthPartFolderName)
    subFolderPath <- file.path(downloadedArticlesFolder, subFolderName)
    dir.create(subFolderPath)

    # write articles.urls for each 10K folders that contains 10K articles urls
    writeLines(articlesLinks[firstFileInGroup:lastFileInGroup], 
               file.path(subFolderPath, "articles.urls"))
    
    # add command line in CMD file as:
    # START wget --warc-file=warc\000001-010000 -i 000001-010000\list.urls -P 000001-010000
    cmdCode <-paste0("START ..\\wget --warc-file=warc\\", subFolderName," -i ", 
                     subFolderName, "\\", "articles.urls -P ", subFolderName)
    cmdCodeAll <- c(cmdCodeAll, cmdCode)
  }
  
  cmdFile <- file.path(downloadedArticlesFolder, "start.cmd")
  writeLines(cmdCodeAll, cmdFile)
  print(paste0("Run ", cmdFile, " to start downloading."))
  print("wget.exe should be placed in working directory.")
  
}

## STEP 3. Parse downloaded articles
# Create CMD file for parallel articles parsing.
# Parsing process takes about 1 hour
CreateCMDForParsing <- function() {
  # get list of folders that contain downloaded articles
  folders <- list.files(downloadedArticlesFolder, full.names = FALSE, 
                        recursive = FALSE, pattern = "-")
  
  # create CMD contains commands to run parse.R script with specified folder number
  nn <- 1:length(folders)
  cmdCodeAll <- paste0("start C:/R/R-3.4.0/bin/Rscript.exe ", 
                       file.path(getwd(), "parse.R "), nn)
  
  cmdFile <- file.path(downloadedArticlesFolder, "parsing.cmd")
  writeLines(cmdCodeAll, cmdFile)
  print(paste0("Run ", cmdFile, " to start parsing."))
  
}

# Parse srecific file
ReadFile <- function(filename) {
  
  pg <- read_html(filename, encoding = "UTF-8")
  
  metaTitle <- html_nodes(pg, xpath=".//meta[@property='og:title']") %>%
    html_attr("content") %>% 
    SetNAIfZeroLength() 
  metaType <- html_nodes(pg, xpath=".//meta[@property='og:type']") %>% 
    html_attr("content") %>% 
    SetNAIfZeroLength()
  metaDescription <- html_nodes(pg, xpath=".//meta[@property='og:description']") %>% 
    html_attr("content") %>% 
    SetNAIfZeroLength()
  rubric <- html_nodes(pg, xpath=".//div[@class='b-subheader__title js-nav-opener']") %>% 
    html_text() %>% 
    SetNAIfZeroLength()
  
  scriptContent <- html_nodes(pg, xpath=".//script[contains(text(),'chapters: [')]") %>% 
    html_text() %>% 
    strsplit("\n") %>% 
    unlist()
  
  if (is.null(scriptContent[1])) {
    chapters <- NA
  } else if (is.na(scriptContent[1])) {
    chapters <- NA
  } else {
    chapters <- scriptContent[grep("chapters: ", scriptContent)] %>% unique()
  }
  
  title <- html_nodes(pg, xpath=".//head/title") %>% 
    html_text() %>% 
    SetNAIfZeroLength()
  
  #shareFB <- html_nodes(pg, xpath=".//div[@data-target='fb']")
  #shareVK <- html_nodes(pg, xpath=".//div[@data-target='vk']")
  #shareOK <- html_nodes(pg, xpath=".//div[@data-target='ok']")
  #shareTW <- html_nodes(pg, xpath=".//div[@data-target='tw']")
  #shareLJ <- html_nodes(pg, xpath=".//div[@data-target='LJ']")

  articleBodyNode <- html_nodes(pg, xpath=".//div[@itemprop='articleBody']")
  
  plaintext <- html_nodes(articleBodyNode, xpath=".//p") %>% 
    html_text() %>% 
    paste0(collapse="") 
  if (plaintext == "") {
    plaintext <- NA
  }
  
  plaintextLinks <- html_nodes(articleBodyNode, xpath=".//a") %>% 
    html_attr("href") %>% 
    unique() %>% 
    paste0(collapse=" ")
  if (plaintextLinks == "") {
    plaintextLinks <- NA
  }
  
  additionalLinks <- html_nodes(pg, xpath=".//section/div[@class='item']/div/..//a") %>% 
    html_attr("href") %>% 
    unique() %>% 
    paste0(collapse=" ")
  if (additionalLinks == "") {
    additionalLinks <- NA
  }
  
  imageNodes <- html_nodes(pg, xpath=".//div[@class='b-topic__title-image']")
  imageDescription <- html_nodes(imageNodes, xpath="div//div[@class='b-label__caption']") %>% 
    html_text() %>% 
    unique() %>% 
    SetNAIfZeroLength()
  imageCredits <- html_nodes(imageNodes, xpath="div//div[@class='b-label__credits']") %>% 
    html_text() %>% 
    unique() %>% 
    SetNAIfZeroLength() 
  
  if (is.na(imageDescription)&is.na(imageCredits)) {
    videoNodes <- html_nodes(pg, xpath=".//div[@class='b-video-box__info']")
    videoDescription <- html_nodes(videoNodes, xpath="div[@class='b-video-box__caption']") %>% 
      html_text() %>% 
      unique() %>% 
      SetNAIfZeroLength()
    videoCredits <- html_nodes(videoNodes, xpath="div[@class='b-video-box__credits']") %>% 
      html_text() %>% 
      unique() %>% 
      SetNAIfZeroLength() 
  } else {
    videoDescription <- NA
    videoCredits <- NA
  }
  
  url <- html_nodes(pg, xpath=".//head/link[@rel='canonical']") %>% 
    html_attr("href") %>% 
    SetNAIfZeroLength()
  
  authorSection <- html_nodes(pg, xpath=".//p[@class='b-topic__content__author']")
  authors <- html_nodes(authorSection, xpath="//span[@class='name']") %>% 
    html_text() %>% 
    SetNAIfZeroLength()
  if (length(authors) > 1) {
    authors <- paste0(authors, collapse = "|")
  }
  authorLinks <- html_nodes(authorSection, xpath="a") %>% html_attr("href") %>% SetNAIfZeroLength()
  if (length(authorLinks) > 1) {
    authorLinks <- paste0(authorLinks, collapse = "|")
  }
  
  #itemprop="datePublished"
  #.//time[@class='g-date']
  datetimeString <- html_nodes(pg, xpath=".//div[@class='b-topic__info']/time[@class='g-date']") %>% 
    html_text() %>% 
    unique() %>% 
    SetNAIfZeroLength()
  datetime <- html_nodes(pg, xpath=".//div[@class='b-topic__info']/time[@class='g-date']") %>% 
    html_attr("datetime") %>% unique() %>% SetNAIfZeroLength()
  if (is.na(datetimeString)) {
    datetimeString <- html_nodes(pg, xpath=".//div[@class='b-topic__date']") %>% 
      html_text() %>% 
      unique() %>% 
      SetNAIfZeroLength()
  }

  data.frame(url = url,
             filename = filename, 
             metaTitle= metaTitle,
             metaType= metaType,
             metaDescription= metaDescription,
             rubric= rubric,
             chapters = chapters,
             datetime = datetime,
             datetimeString = datetimeString,
             title = title, 
             plaintext = plaintext, 
             authors = authors, 
             authorLinks = authorLinks,
             plaintextLinks = plaintextLinks,
             additionalLinks = additionalLinks, 
             imageDescription = imageDescription,
             imageCredits = imageCredits,
             videoDescription = videoDescription,
             videoCredits = videoCredits,
             stringsAsFactors=FALSE)
  
}

# Read and parse files in folder with provided number
ReadFilesInFolder <- function(folderNumber) {

  folders <- list.files(downloadedArticlesFolder, full.names = FALSE, 
                        recursive = FALSE, pattern = "-")
  folderName <- folders[folderNumber]
  currentFolder <- file.path(downloadedArticlesFolder, folderName)
  files <- list.files(currentFolder, full.names = TRUE, 
                      recursive = FALSE, pattern = "index")
  
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
  write.csv(df, file.path(parsedArticlesFolder, paste0(folderName, ".csv")), 
            fileEncoding = "UTF-8")
}

## STEP 4. Prepare combined articles data
# Read all parsed csv and combine them in one
UnionData <- function() {
  files <- list.files(parsedArticlesFolder, full.names = TRUE, recursive = FALSE)
  dfList <- c()
  for (i in 1:length(files)) {
    file <- files[i]
    print(file)
    dfList[[i]] <- read.csv(file, stringsAsFactors = FALSE, encoding = "UTF-8")
  }
  df <- bind_rows(dfList)
  write.csv(df, file.path(parsedArticlesFolder, "untidy_articles_data.csv"), 
            fileEncoding = "UTF-8")
}