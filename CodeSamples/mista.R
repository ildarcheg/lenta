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

mainFolder <- "~/mista"
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
        url <- paste0("http://www.forum.mista.ru/topic.php?id=", seq(1:780000), "&all=1")
        writeLines(url, "download/mista.urls")
        return(url)
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

ReadFile <- function(filename) {
        
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

ReadFiles <- function() {
        
        files <- list.files(file.path(mainFolder, downloadFolder),, full.names=T, recursive = TRUE)
        files <- files[2:length(files)]
       ## df <- map_df(files, ReadFile)
        li <- c()
        for (i in 1:length(files)) {
                print(files[i])
                filename <- files[i]
                doc <- htmlTreeParse(filename, useInternalNodes=T) 
                nodeLi <- getNodeSet(doc,"//ol[@id='memberActivity'][@class='activityList']/..//li")
                if (is.null(nodeLi)) {next}
                for (k in 1:length(nodeLi)) {
                        li <- c(li, saveXML(nodeLi[[k]]))        
                }
        }
       saveRDS(li, "li.rds") 
       return(li)
}
