## Run GetArchivePagesURL()
## Run PrepareBulkDownload()

library(RCurl)
library(XML)
library(stringr)
library(dplyr)
library(digest)
library(purrr)
library(rvest)
library(lubridate)
library(warc)
library(selectr)

mainFolder <- "~/330"
tempFolder <- "temp"
downloadFolder <- "download"

BaseURL <- "forum330.com/forum/member/activity/339/"

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

GetWebPageList <- function() {
        
        webpageList <- list()
        
        for (i in 1:168) {
                url <- paste0(BaseURL, i)
                webpage <- GetWebPage(url)
                webpageList[[i]] <- webpage
                if (is.na(webpage)) {
                        next
                }
        }
        
        return(webpageList)
        
}

GetData <- function() {
        
        webpageList <- GetWebPageList()
        saveRDS(webpageList, paste0(file.path(mainFolder, tempFolder), "webpageList.rds"))
}

GetListForScript <- function() {
        url <- c()
        for (i in 1:2358) {
                url <- c(url, paste0("http://forum330.com/forum/member/statistics/", i))
        }  
        writeLines(url, "download/330.urls")
}


AddUserToDataFrame <- function(filename) {

        pg <- read_html(filename, encoding = "UTF-8")
        stat <- html_nodes(pg, xpath=".//div[@id='memberStatistics']/..//li/div") %>% html_text()
        stat <- gsub(pattern = ",", replacement = "", x = stat, fixed = TRUE)
        stat <- gsub(pattern = "\n", replacement = "", x = stat, fixed = TRUE)
        messages <- as.integer(stat[1])
        topics <- as.integer(stat[2])
        topicsin <- as.integer(stat[3])
        name <- html_nodes(pg, xpath=".//h1[@id='memberName']") %>% html_text() 
        registration <- dmy_hms(stat[5]) 
        firstMessage <- dmy_hms(stat[4]) 
        
        id <- html_nodes(pg, xpath=".//li[@class='item-about']/a") %>% html_attr("href")       
        id <- gsub(pattern = "/forum/member/about/", replacement = "", x = id, fixed = TRUE)
        id <- as.integer(id)
        
        data.frame(id = id, name = name, messages = messages, topics = topics, topicsin = topicsin, registration = registration, firstMessage = firstMessage, stringsAsFactors=FALSE)
        
}

GetListUsers <- function() {
        
        files <- list.files(file.path(mainFolder, downloadFolder), , full.names=T, recursive = TRUE)
        df <- map_df(files, AddUserToDataFrame)
        saveRDS(df, "users.rds")
        return(df)
}

GetActivityListForScript <- function() {
        df <- readRDS("users.rds")
        url <- c()
        for (i in 1:nrow(df)) {
                
                id <- df[i, ]$id
                if (df[i, ]$messages == 0) { next }
                mesN <- ceiling(df[i, ]$messages/10)
                
                for (k in 1:mesN) {
                        url <- c(url, paste0("http://forum330.com/forum/member/activity/", df[i, ]$id, "/", k))
                }
        }  
        writeLines(url, "download/330activity.urls")
} 

GetActivityListForScriptTest <- function() {
        df <- readRDS("users.rds")
        url <- c()
        for (i in 1:nrow(df)) {
                
                id <- df[i, ]$id
                if (df[i, ]$messages == 0) { next }
                mesN <- ceiling(df[i, ]$messages/10)
                
                for (k in mesN:1) {
                       ## url <- paste0("http://forum330.com/forum/member/activity/", df[i, ]$id, "/", k)
                        url <- paste0("http://forum330.com/forum/member/activity/", df[i, ]$id, "/", k)
                        print(url)
                        doc <- htmlTreeParse(url, useInternalNodes=T) 
                        ##print(doc)
                        nodeLi <- getNodeSet(doc,"//ol[@id='memberActivity'][@class='activityList']/..//li")
                        if (!is.null(nodeLi)) {break}
                }
        }  

} 

## READ FILES
ReadMessage <- function(messageNode) {

        messages <- html_nodes(messageNode, xpath=".//p[not(child::a)]") %>% html_text() %>% paste0(collapse="\n") 
        repTopicsAddInfo <- html_node(messageNode, xpath=".//a[@rel='post']") %>% html_attr("href")
        repTopicsAddInfo <- gsub("/forum/conversation/post/", "", repTopicsAddInfo)
        repTopicsAddInfo <- str_split(repTopicsAddInfo, "-", simplify = TRUE)
        repTopicIDs <- repTopicsAddInfo[ , 1]
        repMessageIDs <- repTopicsAddInfo[ , 2]
        
        return(list(message = message, repTopicsAddInfo = repTopicsAddInfo))
}

ReadMessages <- function(pg) {
        messagesTotal <- html_nodes(pg, xpath=".//ol[@id='memberActivity'][@class='activityList']/..//li/..//div[@class='activityBody postBody thing']") 
        messages <- c()
        replyToIDs <- c()

        for (i in 1:length(messagesTotal)) {
                messageNode <- messagesTotal[i]      
                message <- messageNode %>% html_nodes(xpath=".//p/text()[normalize-space()]") %>% html_text()
                message <- message[substring(message, 1, 1) != " "]
                message <- paste(message, collapse = " ")
                repTopicsInfo <- html_node(messageNode, xpath=".//a[@rel='post']") %>% html_attr("href")
                repTopicsInfo <- gsub("/forum/conversation/post/", "", repTopicsInfo)
                replyToIDs <- c(replyToIDs, repTopicsInfo)
                messages <- c(messages, message)
        }  
        
        topics <- html_nodes(pg, xpath=".//ol[@id='memberActivity'][@class='activityList']/..//div[@class='action']/a") %>% html_text()
        topicsAddInfo <- html_nodes(pg, xpath=".//ol[@id='memberActivity'][@class='activityList']/..//div[@class='action']/a") %>% html_attr("href")
        topicsAddInfo <- gsub("/forum/conversation/post/", "", topicsAddInfo)
        topicsAddInfo <- str_split(topicsAddInfo, "-", simplify = TRUE)
        topicIDs <- topicsAddInfo[ , 1]
        messageIDs <- topicsAddInfo[ , 2]
        
}

GetMessagesContent <- function(filename) {
        
        doc <- htmlTreeParse(filename, useInternalNodes=T)    
        getNodeSet(doc,"//ol[@id='memberActivity'][@class='activityList']/..//li/..//div[@class='activityBody postBody thing']")
        nodeQuotes <- getNodeSet(doc,"//ol[@id='memberActivity'][@class='activityList']/..//li/..//div[@class='activityBody postBody thing']/..//blockquote")
        nodeReplies <- getNodeSet(doc,"//ol[@id='memberActivity'][@class='activityList']/..//li/..//div[@class='activityBody postBody thing']/..//a[@rel='post']")
}

ReadFileTemp <- function(filename) {
        
        pg <- read_html(filename, encoding = "UTF-8")
        ## http://forum330.com/forum/conversation/post/4659-2
        ## <h1 id='memberName'>andrewks</h1>
        name <- html_nodes(pg, xpath=".//h1[@id='memberName']") %>% html_text()
        ## <ol id='memberActivity' class='activityList'>
        times <- html_nodes(pg, xpath=".//ol[@id='memberActivity'][@class='activityList']/..//li/..//span[@class='time']") %>% html_text()
        actions <- html_nodes(pg, xpath=".//ol[@id='memberActivity'][@class='activityList']/..//div[@class='action']") %>% html_text()
        messages <- ReadMessages(pg)
        topics <- html_nodes(pg, xpath=".//ol[@id='memberActivity'][@class='activityList']/..//div[@class='action']/a") %>% html_text()
        topicsAddInfo <- html_nodes(pg, xpath=".//ol[@id='memberActivity'][@class='activityList']/..//div[@class='action']/a") %>% html_attr("href")
        topicsAddInfo <- gsub("/forum/conversation/post/", "", topicsAddInfo)
        topicsAddInfo <- str_split(topicsAddInfo, "-", simplify = TRUE)
        topicIDs <- topicsAddInfo[ , 1]
        messageIDs <- topicsAddInfo[ , 2]
        ##rel='post'
        tempnode <- html_nodes(pg, xpath=".//ol[@id='memberActivity'][@class='activityList']/..//li/..//div[@class='activityBody postBody thing']/p")

        
        data.frame(name = name, 
                   times = times, 
                   actions = actions,
                   messages = messages, 
                   topics = topics,
                   topicIDs = topicIDs,
                   messageIDs = messageIDs,
                   repTopicIDs = repTopicIDs,
                   repMessageIDs = repMessageIDs, 
                   stringsAsFactors=FALSE)
        
} 

ReadFile <- function(liOne) {
        
        doc <- htmlTreeParse(liOne, useInternalNodes=T, encoding = "UTF-8")    
        node <- getNodeSet(doc,"//div[@class='action']/..//b")
        name <- xmlValue(node[[1]])
        node <- getNodeSet(doc,"//div[@class='activityBody postBody thing']")
        NoBody <- is.list(node) & length(node) == 0      
        
        if (NoBody) {
                df <- GetEmptyDF()
                return(df)
        }
        
        nodeQuotes <- getNodeSet(doc,"//div[@class='activityBody postBody thing']/..//blockquote")
        nodeReplies <- getNodeSet(doc,"//div[@class='activityBody postBody thing']/..//a[@rel='post']")    
        NoReplies <- is.list(nodeReplies) & length(nodeReplies) == 0
        
        replyToTopicID <- NA
        replyToMessageID <- NA 
        
        if (!NoReplies) {
                replyID <- xmlSApply(nodeReplies, xmlGetAttr, "data-id")[1] 
                if (!is.na(replyID)) {
                        replyID <- strsplit(replyID, '-')  
                        replyToTopicID <- as.integer(replyID[[1]][1])
                        replyToMessageID <- as.integer(replyID[[1]][2])
                }
                
        }
        removeNodes(nodeReplies)
        removeNodes(nodeQuotes)

        nodeLinks <- getNodeSet(doc,"//div[@class='activityBody postBody thing']/..//a[@class='link-external']") 
        if (length(nodeLinks) == 0) {
                links <- NA
        } else {
                links <- xmlSApply(nodeLinks, xmlGetAttr, "href")
                links <- paste(links, "", collapse = "|")      
        }
        
        message <- xmlValue(node[[1]])
        message <- gsub("\r","", message)
        message <- gsub("\n","", message)
        node <- getNodeSet(doc,"//span[@class='time']")
        time <- xmlValue(node[[1]])
        time <- dmy_hms(time)
        node <- getNodeSet(doc,"//div[@class='action']")
        action <- xmlValue(node[[1]])
        node <- getNodeSet(doc,"//div[@class='action']/a")
        topic <- xmlValue(node[[1]])
        topicInfo <- xmlSApply(node, xmlGetAttr, "href")[1] 
        topicInfo <- gsub("/forum/conversation/post/", "", topicInfo)
        topicInfo <- str_split(topicInfo, "-")
        topicID <- as.integer(topicInfo[[1]][1])
        messageID <- as.integer(topicInfo[[1]][2])
        
        df <- data.frame(name = name, 
                   time = time, 
                   action = action,
                   message = message, 
                   topic = topic,
                   topicID = topicID,
                   messageID = messageID,
                   replyToTopicID = replyToTopicID,
                   replyToMessageID = replyToMessageID, 
                   links = links,
                   stringsAsFactors=FALSE)
        return(df)        
}

GetEmptyDF <- function() {
        
        df <- data.frame(name = NA, 
                         time = NA, 
                         action = NA,
                         message = NA, 
                         topic = NA,
                         topicID = NA,
                         messageID = NA,
                         replyToTopicID = NA,
                         replyToMessageID = NA, 
                         links = NA,
                         stringsAsFactors=FALSE)
        return(df)
}

RF <- function(liOne) {
        
        df <- tryCatch(ReadFile(liOne), error = GetEmptyDF()) 
        
}

ReadFiles <- function(num) {
        
        li <- readRDS("li.rds")
        df <- GetEmptyDF()
        num <- min(num, length(li))
        liT <- li[1:num]
        df <- map_df(liT, RF)
        saveRDS(df, "df_3.rds")
        return(df)
        
}

ReadFilesTemp <- function() {
        
        files <- list.files(file.path(mainFolder, downloadFolder),, full.names=T, recursive = TRUE)
        ##files <- files[2:length(files)]
       ## df <- map_df(files, ReadFile)
        li <- c()
        l1 <- c()
        l2 <- c()
        for (i in 1:length(files)) {
                print(files[i])
                filename <- files[i]
                doc <- htmlTreeParse(filename, useInternalNodes=T) 
                ##//ol[@id='memberActivity'][@class='activityList']
                nodeLi <- getNodeSet(doc,"//ol[@id='memberActivity'][@class='activityList']/..//li")
                ##nodeLi <- getNodeSet(doc,"//ol[@id='conversationPosts'][@class='postList']/..//li")
                if (is.null(nodeLi)) {
                        l2 <- c(l2, filename)
                        next}
                l1 <- c(l1, filename)
                for (k in 1:length(nodeLi)) {
                        li <- c(li, saveXML(nodeLi[[k]]))        
                }
        }
       saveRDS(li, "li.rds") 
       return(list(li = li, l1 = l1, l2 = l2))
}

GetUsingWARC <- function() {
        warc_dir <- file.path(tempdir(), "330warc")
        dir.create(warc_dir)
        
        urls <- readLines("temp.urls")
        
        create_warc(urls, warc_dir, warc_file="rfolks-warc")
        
        cdx <- read_cdx(file.path(warc_dir, "rfolks-warc.cdx"))
        
        sites <- map(1:nrow(cdx),
                     ~read_warc_entry(file.path(cdx$warc_path[.], cdx$file_name[.]), 
                                      cdx$compressed_arc_file_offset[.]))
        
        map(sites, ~read_html(content(., as="text", encoding="UTF-8"))) %>% 
                map_chr(~html_text(html_nodes(., "title")))        
}
## wget --warc-file=lenta --no-check-certificate -i 330activity.urls



WorkWithData <- function() {
        
        df <- readRDS("df_1.rds")
        dt <- as.tbl(df) 
        dt <- dt[!is.na(dt$name), ] %>% arrange(topicID, messageID)
  
}
