
library(RCurl)
library(XML)
library(stringr)
library(dplyr)
library(digest)

mainFolder <- "~/lentaProject"
tempFolder <- "temp"

if (!file.exists(file.path(mainFolder))) {
        dir.create(file.path(mainFolder))
}
setwd(file.path(mainFolder))
if (!file.exists(file.path(mainFolder, tempFolder))) {
        dir.create(file.path(mainFolder, tempFolder))
}

BaseURL <- "https://lenta.ru"

## DOWNLOAD PLAN TEXT
GetWebPage <- function(address) {
        
        webpage <- NA
        attemptNumber <- 0

        while (is.na(webpage) & attemptNumber < 10) {
                
                attemptNumber <- attemptNumber + 1
                print(paste("   Gerring url attempt", attemptNumber, address))
                webpage <- tryCatch(getURL(address,followLocation = TRUE,.opts = list(timeout = 10)), error = function(x) {return(NA)})
                if (is.na(webpage)) {print("   ERROR")}

        }
        
        return(webpage)
}

GetPageText <- function(address) {
  
        webpage <- GetWebPage(address)
        #getURL(address,followLocation = TRUE,.opts = list(timeout = 10))
        if (is.na(webpage)) {return(NA)}
        pagetree <- htmlTreeParse(webpage, error = function(...) {}, useInternalNodes = TRUE, encoding = "UTF-8")
        node <- getNodeSet(pagetree, "//div[@itemprop='articleBody']/..//p")
        plantext <- xmlSApply(node, xmlValue)
        plantext <- paste(plantext, collapse = "")
        node <- getNodeSet(pagetree, "//title")
        title <- xmlSApply(node, xmlValue)
        
        return(list(plantext = plantext, title = title))
}

GetArchivePageURL <- function(address) {
        print("   getting url...")
        webpage <- GetWebPage(address)
        if (is.na(webpage)) {
                return(c())
        }
        ##webpage <- getURL(address, followLocation = TRUE, .opts = list(timeout = 10))
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
        DateSeq <- seq(as.Date("2010/1/1"), as.Date("2016/8/26"), "days")
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

DownloadPlanText <- function() {
        urls <- readRDS(paste0(file.path(mainFolder, tempFolder), "/urls.rds"))
        #urls <- paste0(BaseURL, urls[1:100])
        urls <- urls[1:1000]

        plantexts <- vector()
        
        df <- data.frame(
                url = character(),
                text = character(),
                title = character(),
                stringsAsFactors = FALSE
        )
        
        for (i in 1:length(urls)) {
                ##for (i in 1:200) {
                
                pageurl <- paste0(BaseURL, urls[i])
                print(pageurl)
                print(paste(i, "of", length(urls)))
                l <- GetPageText(pageurl)
                if (is.na(l)) {next}
                df <- rbind(
                        df,
                        data.frame(
                                pageurl,
                                l$plantext[1],
                                l$title[1],
                                stringsAsFactors = FALSE
                        )
                )
                saveRDS(df, paste0(
                        file.path(mainFolder, tempFolder),
                        "/data.rds"
                ))
        }
        
        names(df) <- c("url", "text", "title")
        saveRDS(df, paste0(file.path(mainFolder, tempFolder), "/data.rds"))
        
        return(df)
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
        #df <- readRDS(paste0(file.path(mainFolder, tempFolder), "/data.rds"))
        df <- readRDS("df10000.rds")
        names(df) <- c("url", "title", "text", "author", "datetime")
        
        df <- df[1:500, ]
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
