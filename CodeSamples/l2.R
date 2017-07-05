library(stringi)
library(RCurl)
library(XML)
library(stringr)
library(dplyr)
library(digest)
library(rvest)

mainFolder <- "~/lenta"
tempFolder <- "temp"

if (!file.exists(file.path(mainFolder))) {
        dir.create(file.path(mainFolder))
}
setwd(file.path(mainFolder))
if (!file.exists(file.path(mainFolder, tempFolder))) {
        dir.create(file.path(mainFolder, tempFolder))
}

BaseURL <- "https://lenta.ru"

ReadFiles <- function() {
        files <- list.files("t", "*html*", full.names=T)
        firstT <- TRUE
        num <- length(files)
        
        limit <- 10000
        num <- limit
        
        df <- data.frame(url = character(), title = character(), plaintext = character(), author = character(), datetime = character(), stringsAsFactors=FALSE)
        
        pb <- txtProgressBar(min = 0, max = num, style = 3)
        for (i in 1:num) {
                setTxtProgressBar(pb, i)
                pg <- read_html(files[i], encoding = "UTF-8")
                plaintext <- html_nodes(pg, xpath=".//div[@itemprop='articleBody']/..//p") %>% html_text() %>% paste0(collapse="") 
                title <- html_text(html_nodes(pg, xpath=".//head/title"))
                url <- html_attr(html_nodes(pg, xpath=".//head/link[@rel='canonical']"), "href")
                author <- html_text(html_nodes(pg, xpath=".//span[@class='name']"))
                authorLength <- length(author)
                if (authorLength==0) {author <- ""}
                datetime <- html_text(html_nodes(pg, xpath=".//div[@class='b-topic__info']/..//time[@class='g-date']"))
                
                dfT <- data.frame(url=url, title = title, plaintext = plaintext, author = author, datetime = datetime, stringsAsFactors=FALSE)
                df <- rbind(df, dfT)
        }
        return(df)
}

ReadFilesTest <- function() {
        files <- list.files("t", "*html*", full.names=T)
        firstT <- TRUE
        num <- length(files)
        
        df <- data.frame(url = character(), title = character(), plaintext = character(), author = character(), datetime = character(), stringsAsFactors=FALSE)
        
        pb <- txtProgressBar(min = 0, max = num, style = 3)
        urls <- c()
        for (i in 1:num) {
                setTxtProgressBar(pb, i)
                pg <- read_html(files[i], encoding = "UTF-8")
                url <- html_attr(html_nodes(pg, xpath=".//head/link[@rel='canonical']"), "href")
                urls <- c(urls, url)
        }
        return(urls)  
        
        for (i in 1:10) {writeLines(x1[((i-1)*10000+1):min((i)*10000, length(x1))], paste0("t", i, "/lenta.urls"))}
}
