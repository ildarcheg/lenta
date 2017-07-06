## Run GetArchivePagesURL()
## Run PrepareBulkDownload()

library(RCurl)
library(XML)
library(stringr)
library(dplyr)
library(digest)
library(purrr)
library(rvest)

mainFolder <- "~/lentaProject"
tempFolder <- "temp"
downloadFolder <- "download"

BaseURL <- "https://lenta.ru"
DateSeq <- seq(as.Date("2016/8/24"), as.Date("2016/8/26"), "days")

if (!file.exists(file.path(mainFolder))) {
        dir.create(file.path(mainFolder))
}
setwd(file.path(mainFolder))
if (!file.exists(file.path(mainFolder, tempFolder))) {
        dir.create(file.path(mainFolder, tempFolder))
}
if (!file.exists(file.path(mainFolder, downloadFolder))) {
        dir.create(file.path(mainFolder, downloadFolder))
}


## GET URLS LIST
GetWebPage <- function(address) {
        
        webpage <- NA
        attemptNumber <- 0
        
        while (is.na(webpage) & attemptNumber < 10) {
                
                attemptNumber <- attemptNumber + 1
                print(paste("   getting url attempt", attemptNumber, address))
                webpage <- tryCatch(getURL(address,followLocation = TRUE,.opts = list(timeout = 10)), error = function(x) {return(NA)})
                if (is.na(webpage)) {print("   ERROR")}
                
        }
        
        return(webpage)
        
}

GetArchivePageURL <- function(address) {
        
        print("   getting url...")
        webpage <- GetWebPage(address)
        if (is.na(webpage)) {
                return(c())
        }
        print(paste(typeof(webpage), class(webpage)))
        print("   parsing html...")
        pagetree <-
                htmlTreeParse(
                        webpage,
                        error = function(...) {
                        },
                        useInternalNodes = TRUE,
                        encoding = "UTF-8"
                )
        print("   getting node...")
        node <- getNodeSet(pagetree, "//div[@class='titles']/..//a")
        print("   getting attribute...")
        url <- xmlSApply(node, xmlGetAttr, "href")
        
        return(url)
        
}

GetArchivePagesURL <- function() {
        DateSeq <- gsub(pattern = "-",
                        replacement = "/",
                        x = DateSeq)
        
        urls <- vector(mode = "character")
        
        for (i in 1:length(DateSeq)) {
                DayArchivePagURL <- paste(BaseURL, DateSeq[i], sep = "/news/")
                print(paste("Getting", DayArchivePagURL))
                DayUrls <- GetArchivePageURL(DayArchivePagURL)
                urls <- c(urls, DayUrls)
                saveRDS(urls, paste0(file.path(mainFolder, tempFolder), "/urls.rds"))
        }
        
        saveRDS(urls, paste0(file.path(mainFolder, tempFolder), "/urls.rds"))
}

## PREPARE CODE FOR BULK DOWNLOAD
PrepareBulkDownload <- function() {
        
        lentaURLs <- readRDS(paste0(file.path(mainFolder, tempFolder), "/urls.rds"))
        lentaURLs <- paste0(BaseURL, lentaURLs)
        writeLines(lentaURLs, paste0(file.path(mainFolder, downloadFolder), "/lenta.urls"))
        print(paste0("Run in terminal: cd ", file.path(mainFolder, downloadFolder)))
        print("Run in terminal: wget --warc-file=lenta --no-check-certificate -i lenta.urls")
        
}

## READ FILES
ReadFile <- function(filename) {
        
        pg <- read_html(filename, encoding = "UTF-8")
        
        plaintext <- html_nodes(pg, xpath=".//div[@itemprop='articleBody']/..//p") %>% 
                html_text() %>% 
                paste0(collapse="") 
        
        title <- html_text(html_nodes(pg, xpath=".//head/title"))
        url <- html_attr(html_nodes(pg, xpath=".//head/link[@rel='canonical']"), "href")
        author <- html_text(html_nodes(pg, xpath=".//span[@class='name']"))
        authorLength <- length(author)
        if (authorLength==0) {author <- ""}
        datetime <- html_nodes(pg, xpath=".//time[@class='g-date']") %>% html_attr("datetime")
        datetimeLength <- length(datetime)
        if (datetimeLength==0) {datetime <- NA}
        
        ##print(paste("Part ", filename))
        data.frame(url = url, title = title, plaintext = plaintext, author = author, datetime = datetime, stringsAsFactors=FALSE)

}

ReadFiles <- function() {
        
        files <- list.files(file.path(mainFolder, downloadFolder), "*html*", full.names=T, recursive = TRUE)
        steps <- trunc(length(files)/20000)+1
        dfList <- list()
        for (i in 1:steps) {
                begin <- (i-1)*20000 + 1
                end <- min(i*20000, length(files))
                print(paste("Start", begin, " ", end))
                dfList[[i]] <- map_df(files[begin:end], ReadFile)
                saveRDS(dfList, "temp/dfList.rds")
        }
        return(dfList)
}

## WORK WITH PLAN TEXT
GenShingle <- function(url, source, shingleLen) {
        
        words <- strsplit(source, " ")[[1]]
        shingleNum <- length(words) - (shingleLen-1)
        shingles <- character()
        shinglesSHA1 <- character()
        for (i in 1:shingleNum) {
                shingle <- paste(words[i:(i+shingleLen-1)], collapse = " ")
                shingleSHA1 <- digest(shingle, algo="sha1")
                shingles <- c(shingles, shingle) 
                shinglesSHA1 <- c(shinglesSHA1, shingleSHA1)
        }
        #print(paste(url, shingles))
        df <- data.frame(
                url = url,
                shingle = shingles,
                shingleSHA1 = shinglesSHA1,
                stringsAsFactors = FALSE
        )
        return(df)
}

PlanTextProcessing <- function() {

        df <- readRDS(paste0(file.path(mainFolder, tempFolder), "/df.rds"))
        
        df <- CanonizePlanText(df)
        
        dfs <- data.frame(
                url = character(),
                shingle = character(),
                shingleSHA1 = character(),
                stringsAsFactors = FALSE
        )
        
        num <- nrow(df)
        pb <- txtProgressBar(min = 0, max = num, style = 3)
        for (i in 1:num) {
                setTxtProgressBar(pb, i)
                dfsl <- GenShingle(df$url[i], df$text[i], 10)
                dfs <- rbind(dfs, dfsl)
        }
        
        dt <- tbl_df(dfs)
        dtS <- dt %>% select(shingleSHA1, url) %>% count(shingleSHA1) %>% filter(n > 1) %>% arrange(n)
        shalist <- dtS$shingleSHA1
        dtTemp <- l$dt %>% filter(shingleSHA1 %in% shalist)
        dtO <- inner_join(dtTemp, dtTemp, "shingle") %>% filter(url.x != url.y)
        saveRDS(paste0(file.path(mainFolder, tempFolder), "/dfSHA1.rds"))
        return(list(dt = dt, dtS = dt, dtO = dtO))
}

CanonizePlanText <- function(df) {
        stop_symbols <-
                c(".",
                  ",",
                  "!",
                  "?",
                  ":",
                  ";",
                  "-",
                  "—",
                  "\n",
                  "(",
                  ")",
                  "«",
                  "»")
        for (i in 1:length(stop_symbols)) {
                df$text <- gsub(stop_symbols[i], " ", df$text, fixed = TRUE)
                df$text <- gsub("   ", " ", df$text, fixed = TRUE)
                df$text <- gsub("  ", " ", df$text, fixed = TRUE)
                df$text <- str_trim(df$text)
        }
        df$text <- tolower(df$text)
        return(df)
}

