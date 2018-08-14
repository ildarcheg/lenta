# Analyze This. Lenta.ru (part 1)
### What
Lenta.ru is a Moscow-based online newspaper in Russian language, owned by Rambler Media Group which belongs to Prof-Media. It is one of the most popular Russian language online resources with over 600 thousand visitors daily. [Wikipedia](https://en.wikipedia.org/wiki/Lenta.ru)


### Grabbing
First of all, I had to decide how to grab a content of the newspaper. Google told that the optimal way for that would be [rvest](https://cran.r-project.org/web/packages/rvest/rvest.pdf) package. This package allows to get plain text content of the page and extract content from a specific field with xPath.

The structure of the website allows us to get all articles for a specific day using this type of link `https://lenta.ru/YEAR/MONTH/DAY/` (as example https://lenta.ru/2017/07/01/).  
For grabbing and parsing purposes I used following packages:
```R
require(lubridate)
require(rvest)
require(dplyr)
require(tidyr)
require(purrr)
require(XML)
require(data.table)
require(stringr)
require(jsonlite)
require(reshape2)
```

Simple code that allows me to get the links to all archive pages for the last 8 years:
```R
articlesStartDate <- as.Date("2010-01-01")
articlesEndDate <- as.Date("2017-06-30")
## STEP 1. Prepare articles links list
# Dowload list of pages with archived articles. 
# Takes about 40 minutes
GetNewsListForPeriod <- function() {
  timestamp()
  # Prepare vector of links of archive pages in https://lenta.ru//yyyy/mm/dd/ format
  dayArray <- seq(as.Date(articlesStartDate), as.Date(articlesEndDate), 
                  by="days")
  archivePagesLinks <- paste0(baseURL, "/", year(dayArray), 
                              "/", formatC(month(dayArray), width = 2, format = "d", flag = "0"), 
                              "/", formatC(day(dayArray), width = 2, format = "d", flag = "0"), 
                              "/")
  # Go through all pages and extract all news links
  articlesLinks <- c()
  for (i in 1:length(archivePagesLinks)) {
    pg <- read_html(archivePagesLinks[i], encoding = "UTF-8")
    
    linksOnPage <- html_nodes(pg, 
                        xpath=".//section[@class='b-longgrid-column']//div[@class='titles']//a") %>% 
      html_attr("href")   
    articlesLinks <- c(articlesLinks, linksOnPage)
    saveRDS(articlesLinks, file.path(tempDataFolder, "tempArticlesLinks.rds"))
  }
  
  # Add root and write down all the news links
  articlesLinks <- paste0(baseURL, articlesLinks)
  writeLines(articlesLinks, file.path(tempDataFolder, "articles.urls"))
  timestamp()
}
```

I a result I got `archivePagesLinks` with links to all archive pages from `2010-01-01` to `2017-06-30`:
```
> head(archivePagesLinks)
[1] "https://lenta.ru/2010/01/01/"
[2] "https://lenta.ru/2010/01/02/"
[3] "https://lenta.ru/2010/01/03/"
[4] "https://lenta.ru/2010/01/04/"
[5] "https://lenta.ru/2010/01/05/"
[6] "https://lenta.ru/2010/01/06/"
> length(archivePagesLinks)
[1] 2738
```

Using `read_html` in a loop I got a content of all archive pages and using `html_nodes` and `html_attr` got links to all the articles (about `400К` at that time):
```
> head(articlesLinks)
[1] "https://lenta.ru/news/2009/12/31/kids/"     
[2] "https://lenta.ru/news/2009/12/31/silvio/"   
[3] "https://lenta.ru/news/2009/12/31/postpone/" 
[4] "https://lenta.ru/photo/2009/12/31/meeting/" 
[5] "https://lenta.ru/news/2009/12/31/boeviks/"  
[6] "https://lenta.ru/news/2010/01/01/celebrate/"
> length(articlesLinks)
[1] 379862
```

Once I finished this step I realized one problem. The code above took about `40 min` to process `2738` links and it was easy to approximate the time I need to process `379862`. About `5550 minutes` or `92.5 hours`... This is beyond awkward. I decided to try `readLines {base}` and `download.file {utils}` and got same result. Tried `htmlParse {XML}` (similar to `read_html`), got the same. Tried `getURL {RCurl}`, same. Dead end.

![](images/dead_end.jpg)

Looking for a solution of the problem I decided to try the tools that can work in parallel because at the time of the performing CPU, RAM and Network were not busy. Google advised to check out `parallel-package {parallel}`. After a few hours of testing, I realized that there is no profit at all. Someone in Google told that it does make a sense to parallel calculation and data manipulation, but the work with HDD or network will be done within one process one by one (I might be wrong in understanding of the situation). Anyway, in case it works the improvements will be related to a number of the cores and even if I had 8 cores (but I didn't) I had to wait about `690 minutes`.

My next idea was to run a few R processes, but didn't find in Google "how to run code in a new R session". Thought about running R script in command line but at that time my experience with CMD was really poor. Dead end again.

At that time I decided to ask the community and I already knew [stackoverflow](https://stackoverflow.com) was the best place for that. I described the [problem](https://stackoverflow.com/questions/39180106/i-have-to-grab-plantext-from-over-290k-webpages-is-there-a-way-to-improve-the-s) and very soon I got a reply from [Bob Rudis](https://rud.is/). I tried his code and it worked. This one problem - I have no idea how it worked. It was a first time I heard of `wget`, I didn't know what to do with `WARC` and why to pass a function to function as an argument. But `when you look long into a code, the code looks into you` and after breaking the code into pieces and run it one by one you have a chance to understand it (sometimes). Google help me again with understanding `wget`. 

The final solution of the problem with help of `wget` was:
```
  wget --warc-file=lenta -i lenta.urls
```

The code itself looks like:
```R
  system("wget --warc-file=lenta -i lenta.urls", intern = FALSE)
```

At the end of performing, I got numerous files with html content of the pages. Also, I got compressed `WARC` file with logs and pages content. The idea of [Bob Rudis](https://rud.is/) was to parse `WARC`. Exactly what I was looking for plus I kept all pages locally.

First performance measurement showed that `2000` links were downloaded for `10 minutes`, that gives roughly `1890 minutes` - almost 3 times faster but still not enough. Decided to step back and try with `parallel-package {parallel}` but didn't find any profits.

Finaly, I decided to do knight's move. Run a few really parallel processes. After spending few days playing with CMD I was ready to run parallel processes out of R. This code helped me to prepare bash script that runs a bunch of `wget` processes simultaneously:
```R
## STEP 2. Prepare wget CMD files for parallel downloading
# Create CMD file.
# Downloading process for 400K pages takes about 3 hours. Expect about 70GB 
# in html files and 12GB in compressed WARC files
CreateWgetCMDFiles <- function() {
  timestamp()
  articlesLinks <- readLines(file.path(tempDataFolder, "articles.urls"))
  dir.create(warcFolder, showWarnings = FALSE)
  
  # Split up articles links array by 10K links 
  numberOfLinks <- length(articlesLinks)
  digitNumber <- nchar(numberOfLinks)
  groupSize <- 10000
  filesGroup <- seq(from = 1, to = numberOfLinks, by = groupSize)
  cmdCodeAll <- c()
  
  for (i in 1:length(filesGroup)) {
    
    # Prepare folder name as 00001-10000, 10001-20000 etc
    firstFileInGroup <- filesGroup[i]
    lastFileInGroup <- min(firstFileInGroup + groupSize - 1, numberOfLinks)
    leftPartFolderName <- formatC(firstFileInGroup, width = digitNumber, 
                                  format = "d", flag = "0")
    rigthPartFolderName <- formatC(lastFileInGroup, width = digitNumber, 
                                   format = "d", flag = "0")
    subFolderName <- paste0(leftPartFolderName, "-", rigthPartFolderName)
    subFolderPath <- file.path(downloadedArticlesFolder, subFolderName)
    dir.create(subFolderPath)
    
    # Write articles.urls for each 10K folders that contains 10K articles urls
    writeLines(articlesLinks[firstFileInGroup:lastFileInGroup], 
               file.path(subFolderPath, "articles.urls"))
    
    # Add command line in CMD file that will looks like:
    # 'START wget --warc-file=warc\000001-010000 -i 000001-010000\list.urls -P 000001-010000'
    cmdCode <-paste0("START ..\\wget -i ", 
                     subFolderName, "\\", "articles.urls -P ", subFolderName)
    # Use commented code below for downloading with WARC files:
    #cmdCode <-paste0("START ..\\wget --warc-file=warc\\", subFolderName," -i ", 
    #                 subFolderName, "\\", "articles.urls -P ", subFolderName)
    
    cmdCodeAll <- c(cmdCodeAll, cmdCode)
  }
  
  # Write down command file
  cmdFile <- file.path(downloadedArticlesFolder, "start.cmd")
  writeLines(cmdCodeAll, cmdFile)
  print(paste0("Run ", cmdFile, " to start downloading."))
  print("wget.exe should be placed in working directory.")
  timestamp()
}
```

This code breaks links array into chunks by 10000 (in my case it was about 38 chunks), creates folders like `00001-10000`, `10001-20000` etc., puts articles.urls (that contains 10000 links) in folders. The final bash script:
```
START ..\wget --warc-file=warc\000001-010000 -i 000001-010000\articles.urls -P 000001-010000
START ..\wget --warc-file=warc\010001-020000 -i 010001-020000\articles.urls -P 010001-020000
START ..\wget --warc-file=warc\020001-030000 -i 020001-030000\articles.urls -P 020001-030000
...
START ..\wget --warc-file=warc\350001-360000 -i 350001-360000\articles.urls -P 350001-360000
START ..\wget --warc-file=warc\360001-370000 -i 360001-370000\articles.urls -P 360001-370000
START ..\wget --warc-file=warc\370001-379862 -i 370001-379862\articles.urls -P 370001-379862
```

This script runs 38 `wget` processes (load on `3.5GHz Xeon E-1240 v5, 32Gb, SSD, Windows Server 2012`):

![Загрузка компьютера в момент одновременной закачки страниц](images/download_performance.jpg)

Performance time was about `180 minutes` or `3 hours`, `10 times` faster than `wget` и `30 times` faster than `read_html {rvest}`.

The result of the script:
```
> indexFiles <- list.files(downloadedArticlesFolder, full.names = TRUE, recursive = TRUE, pattern = "index")
> length(indexFiles)
[1] 379703
> sum(file.size(indexFiles))/1024/1024
[1] 66713.61
> warcFiles <- list.files(downloadedArticlesFolder, full.names = TRUE, recursive = TRUE, pattern = "warc")
> length(warcFiles)
[1] 38
> sum(file.size(warcFiles))/1024/1024
[1] 18770.4
```

`379703` downloaded pages (`66713.61MB`) and `38` compressed `WARC` (`18770.40MB`). I realized that I lost about `159` pages and I knew that have to check out `WARC` in order to find the problem but decided not to do for that time.

### Parsing

Once I decided what information would be interesting for me I prepared this script:
```R
# Parse specific file
ReadFile <- function(filename) {
  
  pg <- read_html(filename, encoding = "UTF-8")
  
  # Extract Title, Type, Description
  metaTitle <- html_nodes(pg, xpath=".//meta[@property='og:title']") %>%
    html_attr("content") %>% 
    SetNAIfZeroLength() 
  metaType <- html_nodes(pg, xpath=".//meta[@property='og:type']") %>% 
    html_attr("content") %>% 
    SetNAIfZeroLength()
  metaDescription <- html_nodes(pg, xpath=".//meta[@property='og:description']") %>% 
    html_attr("content") %>% 
    SetNAIfZeroLength()
  
  # Extract script contect that contains rubric and subrubric data
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
  
  articleBodyNode <- html_nodes(pg, xpath=".//div[@itemprop='articleBody']")
  
  # Extract articles body
  plaintext <- html_nodes(articleBodyNode, xpath=".//p") %>% 
    html_text() %>% 
    paste0(collapse="") 
  if (plaintext == "") {
    plaintext <- NA
  }
  
  # Extract links from articles body 
  plaintextLinks <- html_nodes(articleBodyNode, xpath=".//a") %>% 
    html_attr("href") %>% 
    unique() %>% 
    paste0(collapse=" ")
  if (plaintextLinks == "") {
    plaintextLinks <- NA
  }
  
  # Extract links related to articles
  additionalLinks <- html_nodes(pg, xpath=".//section/div[@class='item']/div/..//a") %>% 
    html_attr("href") %>% 
    unique() %>% 
    paste0(collapse=" ")
  if (additionalLinks == "") {
    additionalLinks <- NA
  }
  
  # Extract image Description and Credits
  imageNodes <- html_nodes(pg, xpath=".//div[@class='b-topic__title-image']")
  imageDescription <- html_nodes(imageNodes, xpath="div//div[@class='b-label__caption']") %>% 
    html_text() %>% 
    unique() %>% 
    SetNAIfZeroLength()
  imageCredits <- html_nodes(imageNodes, xpath="div//div[@class='b-label__credits']") %>% 
    html_text() %>% 
    unique() %>% 
    SetNAIfZeroLength() 
  
  # Extract video Description and Credits
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
  
  # Extract articles url
  url <- html_nodes(pg, xpath=".//head/link[@rel='canonical']") %>% 
    html_attr("href") %>% 
    SetNAIfZeroLength()
  
  # Extract authors
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
  
  # Extract publish date and time
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
             chapters = chapters,
             datetime = datetime,
             datetimeString = datetimeString,
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
```

This code allowed me to parse files in 1 folder (of 38):
```R
folderNumber <- 1
# Read and parse files in folder with provided number
ReadFilesInFolder <- function(folderNumber) {
  timestamp()
  # Get name of folder that have to be parsed
  folders <- list.files(downloadedArticlesFolder, full.names = FALSE, 
                        recursive = FALSE, pattern = "-")
  folderName <- folders[folderNumber]
  currentFolder <- file.path(downloadedArticlesFolder, folderName)
  files <- list.files(currentFolder, full.names = TRUE, 
                      recursive = FALSE, pattern = "index")
  
  # Split files in folder in 1000 chunks and parse them using ReadFile
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
  
  # combine rows in data frame and write down
  df <- bind_rows(dfList)
  write.csv(df, file.path(parsedArticlesFolder, paste0(folderName, ".csv")), 
            fileEncoding = "UTF-8")
}
```
Getting `00001-10000` folder content, breaking into chunks by 1000, I run `ReadFile` for each chunk.

Performance measurement showed that it takes about `8 minutes` for `10000` articles and `300 minutes` or `5 hours` for all.

My second knight's move and the code that prepares bash script for parallel parsing:
```R
## STEP 3. Parse downloaded articles
# Create CMD file for parallel articles parsing.
# Parsing process takes about 1 hour. Expect about 1.7Gb in parsed files
CreateCMDForParsing <- function() {
  timestamp()
  # Get list of folders that contain downloaded articles
  folders <- list.files(downloadedArticlesFolder, full.names = FALSE, 
                        recursive = FALSE, pattern = "-")
  
  # Create CMD contains commands to run parse.R script with specified folder number
  nn <- 1:length(folders)
  cmdCodeAll <- paste0("start C:/R/R-3.4.0/bin/Rscript.exe ", 
                       file.path(getwd(), "parse.R "), nn)
  
  cmdFile <- file.path(downloadedArticlesFolder, "parsing.cmd")
  writeLines(cmdCodeAll, cmdFile)
  print(paste0("Run ", cmdFile, " to start parsing."))
  timestamp()
}
```

Like:
```
start C:/R/R-3.4.0/bin/Rscript.exe C:/Users/ildar/lenta/parse.R 1
start C:/R/R-3.4.0/bin/Rscript.exe C:/Users/ildar/lenta/parse.R 2
start C:/R/R-3.4.0/bin/Rscript.exe C:/Users/ildar/lenta/parse.R 3
...
start C:/R/R-3.4.0/bin/Rscript.exe C:/Users/ildar/lenta/parse.R 36
start C:/R/R-3.4.0/bin/Rscript.exe C:/Users/ildar/lenta/parse.R 37
start C:/R/R-3.4.0/bin/Rscript.exe C:/Users/ildar/lenta/parse.R 38
```

The bash script above run next R script:
```R
args <- commandArgs(TRUE)
n <- as.integer(args[1])

# Set workling directory and locale for macOS and Windows
if (Sys.info()['sysname'] == "Windows") {
  workingDirectory <- paste0(Sys.getenv("HOMEPATH"), "\\lenta") 
  Sys.setlocale("LC_ALL", "Russian")
} else {
  workingDirectory <- ("~/lenta")
  Sys.setlocale("LC_ALL", "ru_RU.UTF-8")
}
setwd(workingDirectory)

source("get_lenta_articles_list.R")

ReadFilesInFolder(n)
```

![CPU Load](images/parse_performance.jpg)

I got `100%` load and `30 minutes`. One more 10 times profit.

And final step of the parsing stage was to combine 38 files:
```R
## STEP 4. Prepare combined articles data
# Read all parsed csv and combine them in one.
# Expect about 1.7Gb in a combined file
UnionData <- function() {
  timestamp()
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
  timestamp()
}
```

`1.739MB` of unclean and untidy data:
```
> file.size(file.path(parsedArticlesFolder, "untidy_articles_data.csv"))/1024/1024
[1] 1739.047
```

What inside?
```
> str(dfM, vec.len = 1)
'data.frame':   379746 obs. of  21 variables:
 $ X.1             : int  1 2 ...
 $ X               : int  1 2 ...
 $ url             : chr  "https://lenta.ru/news/2009/12/31/kids/" ...
 $ filename        : chr  "C:/Users/ildar/lenta/downloaded_articles/000001-010000/index.html" ...
 $ metaTitle       : chr  "Новым детским омбудсменом стал телеведущий Павел Астахов" ...
 $ metaType        : chr  "article" ...
 $ metaDescription : chr  "Президент РФ Дмитрий Медведев назначил нового уполномоченного по правам ребенка в России. Вместо Алексея Голова"| __truncated__ ...
 $ rubric          : logi  NA ...
 $ chapters        : chr  "          chapters: [\"Россия\",\"Страница_подраздела\",\"lenta.ru:_Россия:_Новым_детским_омбудсменом_стал_теле"| __truncated__ ...
 $ datetime        : chr  "2009-12-31T21:24:33Z" ...
 $ datetimeString  : chr  " 00:24,  1 января 2010" ...
 $ title           : chr  "Новым детским омбудсменом стал телеведущий Павел Астахов: Россия: Lenta.ru" ...
 $ plaintext       : chr  "Президент РФ Дмитрий Медведев назначил нового уполномоченного по правам ребенка в России. Вместо Алексея Голова"| __truncated__ ...
 $ authors         : chr  NA ...
 $ authorLinks     : chr  NA ...
 $ plaintextLinks  : chr  "http://www.interfax.ru/" ...
 $ additionalLinks : chr  "https://lenta.ru/news/2009/12/29/golovan/ https://lenta.ru/news/2009/09/01/children/" ...
 $ imageDescription: chr  NA ...
 $ imageCredits    : chr  NA ...
 $ videoDescription: chr  NA ...
 $ videoCredits    : chr  NA ...
```

Parsing is done. Only one step left before we go to clean and tidy stage.

### SOCIAL MEDIA

Let's gather information about how people react to each article. With help of Developer Tools in Google Chrome I found out this request:
```
https://graph.facebook.com/?id=https%3A%2F%2Flenta.ru%2Fnews%2F2017%2F08%2F10%2Fudostov%2F
```

The answer was:
```
{
   "share": {
      "comment_count": 0,
      "share_count": 243
   },
   "og_object": {
      "id": "1959067174107041",
      "description": ...,
      "title": ...,
      "type": "article",
      "updated_time": "2017-08-10T09:21:29+0000"
   },
   "id": "https://lenta.ru/news/2017/08/10/udostov/"
}
```

The same request was found for VK, Odnoklassniki (social networks popular in Russia) and Rambler.

The code that prepares 4 CMD files, that run parallel requests to social networks above:
```R
## STEP 5. Prepare wget CMD files for parallel downloading social
# Create CMD file.
CreateWgetCMDFilesForSocial <- function() {
  timestamp()
  articlesLinks <- readLines(file.path(tempDataFolder, "articles.urls"))
  dir.create(warcFolder, showWarnings = FALSE)
  dir.create(warcFolderForFB, showWarnings = FALSE)
  dir.create(warcFolderForVK, showWarnings = FALSE)
  dir.create(warcFolderForOK, showWarnings = FALSE)
  dir.create(warcFolderForCom, showWarnings = FALSE)
  
  # split up articles links array by 10K links 
  numberOfLinks <- length(articlesLinks)
  digitNumber <- nchar(numberOfLinks)
  groupSize <- 10000
  filesGroup <- seq(from = 1, to = numberOfLinks, by = groupSize)
  cmdCodeAll <- c()
  
  cmdCodeAllFB <- c()
  cmdCodeAllVK <- c()
  cmdCodeAllOK <- c()
  cmdCodeAllCom <- c() 
  
  for (i in 1:length(filesGroup)) {
    
    # Prepare folder name as 00001-10000, 10001-20000 etc
    firstFileInGroup <- filesGroup[i]
    lastFileInGroup <- min(firstFileInGroup + groupSize - 1, numberOfLinks)
    leftPartFolderName <- formatC(firstFileInGroup, width = digitNumber, 
                                  format = "d", flag = "0")
    rigthPartFolderName <- formatC(lastFileInGroup, width = digitNumber, 
                                   format = "d", flag = "0")
    subFolderName <- paste0(leftPartFolderName, "-", rigthPartFolderName)
    
    subFolderPathFB <- file.path(downloadedArticlesFolderForFB, subFolderName)
    dir.create(subFolderPathFB)
    subFolderPathVK <- file.path(downloadedArticlesFolderForVK, subFolderName)
    dir.create(subFolderPathVK)
    subFolderPathOK <- file.path(downloadedArticlesFolderForOK, subFolderName)
    dir.create(subFolderPathOK)
    subFolderPathCom <- file.path(downloadedArticlesFolderForCom, subFolderName)
    dir.create(subFolderPathCom)
    
    # Encode and write down articles.urls for each 10K folders that contains 
    # 10K articles urls.
    # For FB it has to be done in a bit different way because FB allows to pass 
    # up to 50 links as a request parameter.
    
    articlesLinksFB <- articlesLinks[firstFileInGroup:lastFileInGroup]
    numberOfLinksFB <- length(articlesLinksFB)
    digitNumberFB <- nchar(numberOfLinksFB)
    groupSizeFB <- 50
    filesGroupFB <- seq(from = 1, to = numberOfLinksFB, by = groupSizeFB)
    articlesLinksFBEncoded <- c()
    for (k in 1:length(filesGroupFB )) {
      firstFileInGroupFB <- filesGroupFB[k]
      lastFileInGroupFB <- min(firstFileInGroupFB + groupSizeFB - 1, numberOfLinksFB)	
      articlesLinksFBGroup <- paste0(articlesLinksFB[firstFileInGroupFB:lastFileInGroupFB], collapse = ",")
      articlesLinksFBGroup <- URLencode(articlesLinksFBGroup , reserved = TRUE)
      articlesLinksFBGroup <- paste0("https://graph.facebook.com/?fields=engagement&access_token=PlaceYourTokenHere&ids=", articlesLinksFBGroup)
      articlesLinksFBEncoded  <- c(articlesLinksFBEncoded, articlesLinksFBGroup)
    }
    
    articlesLinksVK <- paste0("https://vk.com/share.php?act=count&index=1&url=", 
                              sapply(articlesLinks[firstFileInGroup:lastFileInGroup], URLencode, reserved = TRUE), "&format=json")
    articlesLinksOK <- paste0("https://connect.ok.ru/dk?st.cmd=extLike&uid=okLenta&ref=", 
                              sapply(articlesLinks[firstFileInGroup:lastFileInGroup], URLencode, reserved = TRUE), "")
    articlesLinksCom <- paste0("https://c.rambler.ru/api/app/126/comments-count?xid=", 
                               sapply(articlesLinks[firstFileInGroup:lastFileInGroup], URLencode, reserved = TRUE), "")
    
    writeLines(articlesLinksFBEncoded, file.path(subFolderPathFB, "articles.urls"))
    writeLines(articlesLinksVK, file.path(subFolderPathVK, "articles.urls"))
    writeLines(articlesLinksOK, file.path(subFolderPathOK, "articles.urls"))
    writeLines(articlesLinksCom, file.path(subFolderPathCom, "articles.urls"))
    
    # Add command line in CMD file
    cmdCode <-paste0("START ..\\..\\wget --warc-file=warc\\", subFolderName," -i ", 
                     subFolderName, "\\", "articles.urls -P ", subFolderName, 
                     " --output-document=", subFolderName, "\\", "index")
    
    cmdCodeAll <- c(cmdCodeAll, cmdCode)
  }
  
  cmdFile <- file.path(downloadedArticlesFolderForFB, "start.cmd")
  print(paste0("Run ", cmdFile, " to start downloading."))
  writeLines(cmdCodeAll, cmdFile)
  cmdFile <- file.path(downloadedArticlesFolderForVK, "start.cmd")
  writeLines(cmdCodeAll, cmdFile)
  print(paste0("Run ", cmdFile, " to start downloading."))
  cmdFile <- file.path(downloadedArticlesFolderForOK, "start.cmd")
  writeLines(cmdCodeAll, cmdFile)
  print(paste0("Run ", cmdFile, " to start downloading."))
  cmdFile <- file.path(downloadedArticlesFolderForCom, "start.cmd")
  writeLines(cmdCodeAll, cmdFile)
  print(paste0("Run ", cmdFile, " to start downloading."))
  
  print("wget.exe should be placed in working directory.")
  timestamp()
}
```

Parsing was done with same technics:
```R
## Parse downloaded articles social
ReadSocial <- function() {
  timestamp()
  # Read and parse all warc files in FB folder
  dfList <- list()
  dfN <- 0  
  warcs <- list.files(file.path(downloadedArticlesFolderForFB, "warc"), full.names = TRUE, 
                      recursive = FALSE) 
  for (i in 1:length(warcs)) {
    
    filename <- warcs[i]
    print(filename)
    res <- readLines(filename, warn = FALSE, encoding = "UTF-8")
    anchorPositions <- which(res == "WARC-Type: response")
    responsesJSON <- res[anchorPositions + 28]
    
    getID <- function(responses) { 
      links <- sapply(responses, function(x){x$id}, USE.NAMES = FALSE) %>% unname() 
      links}
    getQuantity <- function(responses) { 
      links <- sapply(responses, function(x){x$engagement$share_count}, USE.NAMES = FALSE) %>% unname() 
      links}    
    for(k in 1:length(responsesJSON)) {
      if(responsesJSON[k]==""){ next }
      responses <- fromJSON(responsesJSON[k])
      if(!is.null(responses$error)) { next }
      links <- sapply(responses, function(x){x$id}, USE.NAMES = FALSE) %>% unname() %>% unlist()
      quantities <- sapply(responses, function(x){x$engagement$share_count}, USE.NAMES = FALSE) %>% unname() %>% unlist() 
      df <- data.frame(link = links, quantity = quantities, social = "FB", stringsAsFactors = FALSE)  
      dfN <- dfN + 1
      dfList[[dfN]] <- df
    }
  }
  dfFB <- bind_rows(dfList)
  
  # Read and parse all warc files in VK folder
  dfList <- list()
  dfN <- 0  
  warcs <- list.files(file.path(downloadedArticlesFolderForVK, "warc"), full.names = TRUE, 
                      recursive = FALSE) 
  for (i in 1:length(warcs)) {
    
    filename <- warcs[i]
    print(filename)
    res <- readLines(filename, warn = FALSE, encoding = "UTF-8")
    anchorPositions <- which(res == "WARC-Type: response")
    links <- res[anchorPositions + 4] %>% 
      str_replace_all("WARC-Target-URI: https://vk.com/share.php\\?act=count&index=1&url=|&format=json", "") %>%
      sapply(URLdecode) %>% unname()
    quantities <- res[anchorPositions + 24] %>% 
      str_replace_all(" |.*\\,|\\);", "") %>%
      as.integer()
    df <- data.frame(link = links, quantity = quantities, social = "VK", stringsAsFactors = FALSE)  
    dfN <- dfN + 1
    dfList[[dfN]] <- df
  }
  dfVK <- bind_rows(dfList)
  
  # Read and parse all warc files in OK folder
  dfList <- list()
  dfN <- 0 
  warcs <- list.files(file.path(downloadedArticlesFolderForOK, "warc"), full.names = TRUE, 
                      recursive = FALSE) 
  for (i in 1:length(warcs)) {
    
    filename <- warcs[i]
    print(filename)
    res <- readLines(filename, warn = FALSE, encoding = "UTF-8")
    anchorPositions <- which(res == "WARC-Type: response")
    links <- res[anchorPositions + 4] %>% 
      str_replace_all("WARC-Target-URI: https://connect.ok.ru/dk\\?st.cmd=extLike&uid=okLenta&ref=", "") %>%
      sapply(URLdecode) %>% unname()
    quantities <- res[anchorPositions + 22] %>% 
      str_replace_all(" |.*\\,|\\);|'", "") %>%
      as.integer()
    df <- data.frame(link = links, quantity = quantities, social = "OK", stringsAsFactors = FALSE)  
    dfN <- dfN + 1
    dfList[[dfN]] <- df
  }
  dfOK <- bind_rows(dfList)
  
  # Read and parse all warc files in Com folder
  dfList <- list()
  dfN <- 0  
  warcs <- list.files(file.path(downloadedArticlesFolderForCom, "warc"), full.names = TRUE, 
                      recursive = FALSE) 
  x <- c()
  for (i in 1:length(warcs)) {
    
    filename <- warcs[i]
    print(filename)
    res <- readLines(filename, warn = FALSE, encoding = "UTF-8")
    anchorPositions <- which(str_sub(res, start = 1, end = 9) == '{"xids":{')
    x <- c(x, res[anchorPositions])
  }  
  for (i in 1:length(warcs)) {
    
    filename <- warcs[i]
    print(filename)
    res <- readLines(filename, warn = FALSE, encoding = "UTF-8")
    anchorPositions <- which(str_sub(res, start = 1, end = 9) == '{"xids":{')
    x <- c(x, res[anchorPositions])
    responses <- res[anchorPositions] %>% 
      str_replace_all('\\{\\"xids\\":\\{|\\}', "")
    if(responses==""){ next }
    links <- str_replace_all(responses, "(\":[^ ]+)|\"", "")
    quantities <- str_replace_all(responses, ".*:", "") %>%
      as.integer()
    df <- data.frame(link = links, quantity = quantities, social = "Com", stringsAsFactors = FALSE)  
    dfN <- dfN + 1
    dfList[[dfN]] <- df
  }
  dfCom <- bind_rows(dfList)
  dfCom <- dfCom[dfCom$link!="",]
  
  # Combine dfs and reshape them into "link", "FB", "VK", "OK", "Com"
  dfList <- list()
  dfList[[1]] <- dfFB
  dfList[[2]] <- dfVK
  dfList[[3]] <- dfOK
  dfList[[4]] <- dfCom
  df <- bind_rows(dfList) 
  dfCasted <- dcast(df, link ~ social, value.var = "quantity")
  dfCasted <- dfCasted[order(dfCasted$link),]
  
  write.csv(dfCasted, file.path(parsedArticlesFolder, "social_articles.csv"), 
            fileEncoding = "UTF-8")
  timestamp()
}
```

Grabbing is done.

### Cleaning

As you could above I have to deal with `379746 obs. of  21 variables and size of 1.739MB` and it is not too fast when you use basic packeges. Google (one more time) advised to try `fread {data.table}`. Feel the differences:
```
> system.time(dfM <- read.csv(untidyDataFile, stringsAsFactors = FALSE, encoding = "UTF-8"))
пользователь      система       прошло 
      133.17         1.50       134.67 
> system.time(dfM <- fread(untidyDataFile, stringsAsFactors = FALSE, encoding = "UTF-8"))
Read 379746 rows and 21 (of 21) columns from 1.698 GB file in 00:00:18
пользователь      система       прошло 
       17.67         0.54        18.22 
```

This code I use to clean and tidy my data:
```R
# Load required packages
require(lubridate)
require(dplyr)
require(tidyr)
require(data.table)
require(tldextract)
require(XML)
require(stringr)
require(tm)

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
parsedArticlesFolder <- file.path(getwd(), "parsed_articles")
tidyArticlesFolder <- file.path(getwd(), "tidy_articles")

# Creare required folders if not exist 
dir.create(tidyArticlesFolder, showWarnings = FALSE)

## STEP 5. Clear and tidy data
# Section 7 takes about 2-4 hours
TityData <- function() {
  
  dfM <- fread(file.path(parsedArticlesFolder, "untidy_articles_data.csv"), 
               stringsAsFactors = FALSE, encoding = "UTF-8")
  # SECTION 1
  print(paste0("1 ",Sys.time()))
  # Remove duplicate rows, remove rows with url = NA, create urlKey column as a key
  dtD <- dfM %>% 
    select(-V1,-X)  %>% 
    distinct(url, .keep_all = TRUE) %>% 
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
  
  # SECTION 2
  print(paste0("2 ",Sys.time()))  
  # Process chapter column to retrive rubric and subrubric
  # Column value such as:
  # chapters: ["Бывший_СССР","Украина","lenta.ru:_Бывший_СССР:_Украина:_Правительство_ФРГ_сочло_неприемлемым_создание_Малороссии"], // Chapters страницы
  # should be represented as rubric value "Бывший СССР" 
  # and subrubric value "Украина"
  dtD <- dtD %>% 
    mutate(chapters = gsub('\"|\\[|\\]| |chapters:', "", chapters)) %>%
    mutate(chaptersFormatted = as.character(sapply(chapters, SplitChapters))) %>%
    separate(col = "chaptersFormatted", into = c("rubric", "subrubric")
             , sep = "\\|", extra = "drop", fill = "right", remove = FALSE) %>%
    filter(!rubric == "NA") %>%
    select(-chapters, -chaptersFormatted) 
  
  # SECTION 3
  print(paste0("3 ",Sys.time()))
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
  
  # SECTION 4
  print(paste0("4 ",Sys.time()))
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
  
  # SECTION 5
  print(paste0("5 ",Sys.time()))  
  # Remove rows with missed datetime values, rename metaTitle to title,
  # remove columns that we do not need anymore  
  dtD <- dtD %>%
    as.data.table() %>%
    na.omit(cols="datetime") %>%
    select(-filename, -metaType, -datetimeString, -datetimeNew) %>%
    rename(title = metaTitle) %>%
    select(url, urlKey, datetime, rubric, subrubric, title, metaDescription, plaintext, 
           authorLinks, additionalLinks, plaintextLinks, imageDescription, imageCreditsPerson,
           imageCreditsCompany, videoDescription, videoCredits)
  
  # SECTION 6
  print(paste0("6 ",Sys.time()))
  # Clean additionalLinks and plaintextLinks
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
  
  # SECTION 7
  print(paste0("7 ",Sys.time()))
  # Clean additionalLinks and plaintextLinks using UpdateAdditionalLinks 
  # function. Links such as:
  # "http://www.dw.com/ru/../B2 https://www.welt.de/politik/.../de/"
  # should be represented as "dw.com welt.de"
  
  # Function UpdateAdditionalLinks is used to process and clean additionalLinks 
  # and plaintextLinks
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
  
  # Function UpdateAdditionalLinksDomain is used to process additionalLinks 
  # and plaintextLinks and retrive source domain name
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
  
  dtD <- dtD %>% 
    mutate(plaintextLinks = mapply(UpdateAdditionalLinks, plaintextLinks, url)) %>%
    mutate(additionalLinks = mapply(UpdateAdditionalLinks, additionalLinks, url))
  
  # Retrive domain from external links using updateAdditionalLinksDomain 
  # function. Links such as:
  # "http://www.dw.com/ru/../B2 https://www.welt.de/politik/.../de/"
  # should be represented as "dw.com welt.de"  
  numberOfLinks <- nrow(dtD)
  groupSize <- 10000
  groupsN <- seq(from = 1, to = numberOfLinks, by = groupSize)
  
  for (i in 1:length(groupsN)) {
    n1 <- groupsN[i]
    n2 <- min(n1 + groupSize - 1, numberOfLinks) 
    dtD$additionalLinks[n1:n2] <- mapply(UpdateAdditionalLinksDomain, dtD$additionalLinks[n1:n2], dtD$url[n1:n2])
    dtD$plaintextLinks[n1:n2] <- mapply(UpdateAdditionalLinksDomain, dtD$plaintextLinks[n1:n2], dtD$url[n1:n2])
  }
  
  # SECTION 8
  print(paste0("8 ",Sys.time()))
  # Clean title, descriprion and plain text. Remove puntuation and stop words.
  # Prepare for the stem step
  stopWords <- readLines("stop_words.txt", warn = FALSE, encoding = "UTF-8")
  
  dtD <- dtD %>% as.tbl() %>% mutate(stemTitle = tolower(title), 
                                                 stemMetaDescription = tolower(metaDescription), 
                                                 stemPlaintext = tolower(plaintext))
  dtD <- dtD %>% as.tbl() %>% mutate(stemTitle = enc2utf8(stemTitle), 
                                                 stemMetaDescription = enc2utf8(stemMetaDescription), 
                                                 stemPlaintext = enc2utf8(stemPlaintext))
  dtD <- dtD %>% as.tbl() %>% mutate(stemTitle = removeWords(stemTitle, stopWords), 
                                                 stemMetaDescription = removeWords(stemMetaDescription, stopWords), 
                                                 stemPlaintext = removeWords(stemPlaintext, stopWords))
  dtD <- dtD %>% as.tbl() %>% mutate(stemTitle = removePunctuation(stemTitle), 
                                                 stemMetaDescription = removePunctuation(stemMetaDescription), 
                                                 stemPlaintext = removePunctuation(stemPlaintext))   
  dtD <- dtD %>% as.tbl() %>% mutate(stemTitle = str_replace_all(stemTitle, "\\s+", " "), 
                                                 stemMetaDescription = str_replace_all(stemMetaDescription, "\\s+", " "), 
                                                 stemPlaintext = str_replace_all(stemPlaintext, "\\s+", " "))    
  dtD <- dtD %>% as.tbl() %>% mutate(stemTitle = str_trim(stemTitle, side = "both"), 
                                                 stemMetaDescription = str_trim(stemMetaDescription, side = "both"), 
                                                 stemPlaintext = str_trim(stemPlaintext, side = "both"))
  # SECTION 9
  print(paste0("9 ",Sys.time()))
  write.csv(dtD, file.path(tidyArticlesFolder, "tidy_articles_data.csv"), fileEncoding = "UTF-8")
  
  # SECTION 10 Finish
  print(paste0("10 ",Sys.time()))
  
  # SECTION 11 Adding social
  dfM <- read.csv(file.path(tidyArticlesFolder, "tidy_articles_data.csv"), stringsAsFactors = FALSE, encoding = "UTF-8")
  dfS <- read.csv(file.path(parsedArticlesFolder, "social_articles.csv"), stringsAsFactors = FALSE, encoding = "UTF-8")
  dt <- as.tbl(dfM)
  dtS <- as.tbl(dfS) %>% rename(url = link) %>% select(url, FB, VK, OK, Com)
  dtG <- left_join(dt, dtS, by = "url")
  write.csv(dtG, file.path(tidyArticlesFolder, "tidy_articles_data.csv"), fileEncoding = "UTF-8")
}
```
I add `time stamp` as `print(paste0("1 ",Sys.time()))` because the code was not fast. Log from my laptop `2.7GHz i5, 16Gb Ram, SSD, macOS 10.12, R version 3.4.0`:
```
[1] "1 2017-07-21 16:36:59"
[1] "2 2017-07-21 16:37:13"
[1] "3 2017-07-21 16:38:15"
[1] "4 2017-07-21 16:39:11"
[1] "5 2017-07-21 16:42:58"
[1] "6 2017-07-21 16:42:58"
[1] "7 2017-07-21 16:43:35"
[1] "8 2017-07-21 18:41:25"
[1] "9 2017-07-21 19:00:32"
[1] "10 2017-07-21 19:01:04"
```

Log from real server `3.5GHz Xeon E-1240 v5, 32Gb, SSD, Windows Server 2012`:
```
[1] "1 2017-07-21 14:36:44"
[1] "2 2017-07-21 14:37:08"
[1] "3 2017-07-21 14:38:23"
[1] "4 2017-07-21 14:41:24"
[1] "5 2017-07-21 14:46:02"
[1] "6 2017-07-21 14:46:02"
[1] "7 2017-07-21 14:46:57"
[1] "8 2017-07-21 18:58:04"
[1] "9 2017-07-21 19:30:27"
[1] "10 2017-07-21 19:35:18"
```

I believe the comments I left are enough to understand the code.

I realized that `UpdateAdditionalLinksDomain` and `tldextract {tldextract}` (where I parse links) is a bottleneck. I decided not to spend additional time for optimization (maybe in a future).

Result:
```
> str(dfM, vec.len = 1)
'data.frame':   379746 obs. of  21 variables:
 $ X.1             : int  1 2 ...
 $ X               : int  1 2 ...
 $ url             : chr  "https://lenta.ru/news/2009/12/31/kids/" ...
 $ filename        : chr  "C:/Users/ildar/lenta/downloaded_articles/000001-010000/index.html" ...
> str(dfM, vec.len = 1)
Classes ‘data.table’ and 'data.frame':  376913 obs. of  19 variables:
 $ url                : chr  "https://lenta.ru/news/2009/12/31/kids/" ...
 $ urlKey             : chr  "httpslentarunews20091231kids" ...
 $ datetime           : chr  "2010-01-01 00:24:33" ...
 $ rubric             : chr  "Россия" ...
 $ subrubric          : chr  NA ...
 $ title              : chr  "Новым детским омбудсменом стал телеведущий Павел Астахов" ...
 $ metaDescription    : chr  "Президент РФ Дмитрий Медведев назначил нового уполномоченного по правам ребенка в России. Вместо Алексея Голова"| __truncated__ ...
 $ plaintext          : chr  "Президент РФ Дмитрий Медведев назначил нового уполномоченного по правам ребенка в России. Вместо Алексея Голова"| __truncated__ ...
 $ authorLinks        : chr  NA ...
 $ additionalLinks    : chr  NA ...
 $ plaintextLinks     : chr  "interfax.ru" ...
 $ imageDescription   : chr  NA ...
 $ imageCreditsPerson : chr  NA ...
 $ imageCreditsCompany: chr  NA ...
 $ videoDescription   : chr  NA ...
 $ videoCredits       : chr  NA ...
 $ stemTitle          : chr  "новым детским омбудсменом стал телеведущий павел астахов" ...
 $ stemMetaDescription: chr  "президент рф дмитрий медведев назначил нового уполномоченного правам ребенка россии вместо алексея голованя про"| __truncated__ ...
 $ stemPlaintext      : chr  "президент рф дмитрий медведев назначил нового уполномоченного правам ребенка россии вместо алексея голованя про"| __truncated__ ...
```

Result:
```
> file.size(file.path(tidyArticlesFolder, "tidy_articles_data.csv"))/1024/1024
[1] 2741.01
```

### REPRODUCIBLE RESEARCH

Before the final step, I decided to check if my research is reproducible and repeat from the beginning with `September 1st, 1999`.

> I am dealing with 700К articles from now.

`2 hours` to download and extract `700К links`:
```R
> head(articlesLinks)
[1] "https://lenta.ru/news/1999/08/31/stancia_mir/"
[2] "https://lenta.ru/news/1999/08/31/vzriv/"      
[3] "https://lenta.ru/news/1999/08/31/credit_japs/"
[4] "https://lenta.ru/news/1999/08/31/fsb/"        
[5] "https://lenta.ru/news/1999/09/01/dagestan/"   
[6] "https://lenta.ru/news/1999/09/01/kirgiz1/"    
> length(articlesLinks)
[1] 702246
```

`4.5 часа` grab 700000 articles for the last 18 years in 70 cocurrent processes:
```
> indexFiles <- list.files(downloadedArticlesFolder, full.names = TRUE, recursive = TRUE, pattern = "index")
> length(indexFiles)
[1] 702246
> sum(file.size(indexFiles))/1024/1024
[1] 123682.1
```

`1 hour` to parse `123GB` in 70 parallel processes. The result is `2.831MB` of unclean and untidy data:
```
> file.size(file.path(parsedArticlesFolder, "untidy_articles_data.csv"))/1024/1024
[1] 3001.875
```

Cleaning took more then 8 hours (`3.5GHz Xeon E-1240 v5, 32Gb, SSD, Windows Server 2012`):
```
[1] "1 2017-07-27 08:21:46"
[1] "2 2017-07-27 08:22:44"
[1] "3 2017-07-27 08:25:21"
[1] "4 2017-07-27 08:30:56"
[1] "5 2017-07-27 08:38:29"
[1] "6 2017-07-27 08:38:29"
[1] "7 2017-07-27 08:40:01"
[1] "8 2017-07-27 15:55:18"
[1] "9 2017-07-27 16:44:49"
[1] "10 2017-07-27 16:53:02"

```

Table ready to be analyzed (`4.5GB`):
```
> file.size(file.path(tidyArticlesFolder, "tidy_articles_data.csv"))/1024/1024
[1] 4534.328
```

### STEMMING

Финальный блок. Осталось привести заголовок, описание и текст статей к нормальному виду, когда `Путин/Путина/Путину/Путиным` (мир ему и благословение) будет приведено к единообразному `Путин` (мир ему и благословение). В этом мне помогла статья [Стемминг текстов на естественном языке](http://r.psylab.info/blog/2015/05/26/text-stemming/) и программа [MyStem](https://tech.yandex.ru/mystem/). Код приведен ниже:
```R
# Load required packages
require(data.table)
require(dplyr)
require(tidyr)
require(stringr)
require(gdata)

# Set workling directory and locale for macOS and Windows
if (Sys.info()['sysname'] == "Windows") {
  workingDirectory <- paste0(Sys.getenv("HOMEPATH"), "\\lenta") 
  Sys.setlocale("LC_ALL", "Russian")
} else {
  workingDirectory <- ("~/lenta")
  Sys.setlocale("LC_ALL", "ru_RU.UTF-8")
}
setwd(workingDirectory)

# Load library that helps to chunk vectors
source("chunk.R")

# Set common variables
tidyArticlesFolder <- file.path(getwd(), "tidy_articles")
stemedArticlesFolder <- file.path(getwd(), "stemed_articles")

# Create required folders if not exist 
dir.create(stemedArticlesFolder, showWarnings = FALSE)

## STEP 6. Stem title, description and plain text
# Write columns on disk, run mystem, read stemed data and add to data.table
StemArticlesData <- function() {
  
  # Read tidy data and keep only column that have to be stemed.
  # Add === separate rows in stem output.
  # dt that takes about 5GB RAM for 700000 obs. of 25 variables
  # and 2.2GB for 700000 obs. of 5 variables as tbl
  timestamp(prefix = "## START reading file ")
  tidyDataFile <- file.path(tidyArticlesFolder, "tidy_articles_data.csv")
  dt <- fread(tidyDataFile, stringsAsFactors = FALSE, encoding = "UTF-8") %>% 
    as.tbl()
  dt <- dt %>% mutate(sep = "===") %>%
    select(sep, X, stemTitle, stemMetaDescription, stemPlaintext)
  
  # Check memory usage 
  print(ll(unit = "MB"))
  
  # Prepare the list that helps us to stem 3 column 
  sectionList <- list()
  sectionList[[1]] <- list(columnToStem = "stemTitle", 
                           stemedColumn = "stemedTitle",
                           sourceFile = file.path(stemedArticlesFolder, 
                                                  "stem_titles.txt"),
                           stemedFile = file.path(stemedArticlesFolder, 
                                                  "stemed_titles.txt"))
  sectionList[[2]] <- list(columnToStem = "stemMetaDescription", 
                           stemedColumn = "stemedMetaDescription",
                           sourceFile = file.path(stemedArticlesFolder, 
                                                  "stem_metadescriptions.txt"),
                           stemedFile = file.path(stemedArticlesFolder, 
                                                  "stemed_metadescriptions.txt"))
  sectionList[[3]] <- list(columnToStem = "stemPlaintext", 
                           stemedColumn = "stemedPlaintext",
                           sourceFile = file.path(stemedArticlesFolder, 
                                                  "stem_plaintext.txt"),
                           stemedFile = file.path(stemedArticlesFolder, 
                                                  "stemed_plaintext.txt"))
  
  timestamp(prefix = "## steming file ")
  # Write the table with sep, X, columnToStem columns and run mystem.
  # It takes about 30 min to process Title, MetaDescription and Plaintext
  # in 700K rows table.
  # https://tech.yandex.ru/mystem/
  for (i in 1:length(sectionList)) {
    write.table(dt[, c("sep","X", sectionList[[i]]$columnToStem)], 
                sectionList[[i]]$sourceFile, 
                fileEncoding = "UTF-8", sep = ",", quote = FALSE, 
                row.names = FALSE, col.names = FALSE)
    system(paste0("mystem -nlc ", sectionList[[i]]$sourceFile, " ", 
                  sectionList[[i]]$stemedFile), intern = FALSE)  
  }
  
  # Remove dt from memory and call garbage collection
  rm(dt)
  gc()
  
  # Check memory usage 
  print(ll(unit = "MB"))
  
  timestamp(prefix = "## process file ")
  # Process stemed files. it takes about 60 min to process 3 stemed files
  for (i in 1:length(sectionList)) {
    stemedText <- readLines(sectionList[[i]]$stemedFile, 
                            warn = FALSE, 
                            encoding = "UTF-8")
    
    # Split stemed text in chunks
    chunkList <- chunk(stemedText, chunk.size = 10000000)
    
    # Clean chunks one by one and remove characters that were added by mystem
    resLines <- c()
    for (j in 1:length(chunkList)) {
      resTemp <- chunkList[[j]] %>% 
        str_replace_all("===,", "===") %>%
        strsplit(split = "\\\\n|,") %>% unlist() %>% 
        str_replace_all("(\\|[^ ]+)|(\\\\[^ ]+)|\\?|,|_", "")
      resLines <- c(resLines, resTemp[resTemp!=""])
    }  
    
    # Split processed text in rows using === added at the beginnig  
    chunkedRes <- chunk(resLines, chunk.delimiter = "===", 
                        fixed.delimiter = FALSE, 
                        keep.delimiter = TRUE)
    
    # Process each row and extract key (row number) and stemed content
    stemedList <- lapply(chunkedRes, 
                         function(x) {
                           data.frame(key = as.integer(str_replace_all(x[1], "===", "")), 
                                      content = paste0(x[2:length(x)], collapse = " "), 
                                      stringsAsFactors = FALSE)})
    
    # Combine all rows in data frame with key and content colums
    sectionList[[i]]$dt <- bind_rows(stemedList)
    colnames(sectionList[[i]]$dt) <- c("key", sectionList[[i]]$stemedColumn)
    
  }
  
  # Remove variables used in loop and call garbage collection
  rm(stemedText, chunkList, resLines, chunkedRes, stemedList)
  gc()
  
  # Check memory usage 
  print(ll(unit = "MB"))
  
  # read tidy data again
  timestamp(prefix = "## reading file (again)")
  dt <- fread(tidyDataFile, stringsAsFactors = FALSE, encoding = "UTF-8") %>% 
    as.tbl()
  
  # add key column as a key and add tables with stemed data to tidy data 
  timestamp(prefix = paste0("## combining tables "))
  dt <- dt %>% mutate(key = X)
  
  dt <- left_join(dt, sectionList[[1]]$dt, by = "key")
  dt <- left_join(dt, sectionList[[2]]$dt, by = "key")
  dt <- left_join(dt, sectionList[[3]]$dt, by = "key")
  
  sectionList[[1]]$dt <- ""
  sectionList[[2]]$dt <- ""
  sectionList[[3]]$dt <- ""
  
  dt <- dt %>% select(-V1, -X, -urlKey, -metaDescription, -plaintext, -stemTitle, -stemMetaDescription, -stemPlaintext, - key)
  
  write.csv(dt, file.path(stemedArticlesFolder, "stemed_articles_data.csv"), fileEncoding = "UTF-8")
  
  file.remove(sectionList[[1]]$sourceFile)
  file.remove(sectionList[[2]]$sourceFile)
  file.remove(sectionList[[3]]$sourceFile)
  file.remove(sectionList[[1]]$stemedFile)
  file.remove(sectionList[[2]]$stemedFile)
  file.remove(sectionList[[3]]$stemedFile)
  
  # Remove dt, sectionList and call garbage collection
  rm(dt)
  gc()
  
  # Check memory usage
  print(ll(unit = "MB"))
  
  timestamp(prefix = "## END ")
}
```

Result of Part 1:
```R
> file.size(file.path(stemedArticlesFolder, "stemed_articles_data.csv"))/1024/1024
[1] 2273.52
> str(x, vec.len = 1)
Classes ‘data.table’ and 'data.frame':	697601 obs. of  21 variables:
 $ V1                   : chr  "1" ...
 $ url                  : chr  "https://lenta.ru/news/1999/08/31/stancia_mir/" ...
 $ datetime             : chr  "1999-09-01 01:58:40" ...
 $ rubric               : chr  "Россия" ...
 $ subrubric            : chr  NA ...
 $ title                : chr  "Космонавты сомневаются в надежности \"\"\"\"\"\"\"\"Мира\"\"\"\"\"\"\"\"" ...
 $ authorLinks          : chr  NA ...
 $ additionalLinks      : chr  NA ...
 $ plaintextLinks       : chr  NA ...
 $ imageDescription     : chr  NA ...
 $ imageCreditsPerson   : chr  NA ...
 $ imageCreditsCompany  : chr  NA ...
 $ videoDescription     : chr  NA ...
 $ videoCredits         : chr  NA ...
 $ FB                   : int  0 0 ...
 $ VK                   : int  NA 0 ...
 $ OK                   : int  0 0 ...
 $ Com                  : int  NA NA ...
 $ stemedTitle          : chr  "космонавт сомневаться надежность мир" ...
 $ stemedMetaDescription: chr  "командир последний экспедиция афанасьев предупреждать мир смочь создавать проблема весь землянин управлять стан"| __truncated__ ...
 $ stemedPlaintext      : chr  "известно агентство ассошиэйтед пресса экипаж последний экспедиция станция мир считать способный выходить контро"| __truncated__ ...
 - attr(*, ".internal.selfref")=<externalptr> 
```

Part 1 [repo](https://github.com/ildarcheg/lenta).
Part 2 [repo](https://github.com/ildarcheg/lentaproject).

