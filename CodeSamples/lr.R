library(stringi)
library(purrr)
library(rvest)
library(dplyr)

setwd("~/lenta/t/")
warc_file <- "lenta.warc"
#' get the number of records in a warc request
warc_request_record_count <- function(warc_file) {
        
        archive <- file(warc_file, open="r")
        
        rec_count <- 0
        
        while (length(line <- readLines(archive, n=1, warn=FALSE)) > 0) {
                if (grepl("^WARC-Type: request", line)) {
                        rec_count <- rec_count + 1
                }
        }
        
        close(archive)
        
        rec_count
}

#' create a warc record index of the responses so we can
#' seek right to them and slurp them up
warc_response_index <- function(warc_file,
                                record_count=warc_request_record_count(warc_file)) {
        
        records <- vector("list", record_count)
        archive <- file(warc_file, open="r")
        
        idx <- 0
        record <- list(url=NULL, pos=NULL, length=NULL)
        in_request <- FALSE
        
        while (length(line <- readLines(archive, n=1, warn=FALSE)) > 0) {
                
                if (grepl("^WARC-Type:", line)) {
                        if (grepl("response", line)) {
                                if (idx > 0) {
                                        records[[idx]] <- record
                                        record <- list(url=NULL, pos=NULL, length=NULL)
                                }
                                in_request <- TRUE
                                idx <- idx + 1
                        } else {
                                in_request <- FALSE
                        }
                }
                
                if (in_request & grepl("^WARC-Target-URI:", line)) {
                        record$url <- stri_match_first_regex(line, "^WARC-Target-URI: (.*)")[,2]
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


#' retrieve an individual response record
get_warc_response <- function(warc_file, pos, length) {
        
        archive <- file(warc_file, open="r")
        
        seek(archive, pos)
        record <- readChar(archive, length)
        
        record <- stri_split_fixed(record, "\r\n\r\n", 2)[[1]]
        names(record) <- c("header", "page")
        
        close(archive)
        
        as.list(record)
        
}

fooo <- function(r) {
        
        resp <- get_warc_response(warc_file, r$pos, r$length)
        
        # the wget WARC response is sticking a numeric value as the first
        # line for URLs from this site (and it's not a byte-order-mark). so,
        # we need to strip that off before reading in the actual response.
        # i'm pretty sure it's the site injecting this and not wget since i
        # don't see it on other test URLs I ran through this for testing.
        
        pg <- read_html(stri_split_fixed(resp$page, "\r\n", 2)[[1]][2])
        
        html_nodes(pg, xpath=".//div[@itemprop='articleBody']/..//p") %>%
                html_text() %>%
                paste0(collapse="") -> plantext
        
        title <- html_text(html_nodes(pg, xpath=".//head/title"))
        
        data.frame(url=r$url, title, plantext, stringsAsFactors=FALSE)
        
}


my1 <- function(filename) {
        print(filename)
        data.frame(filename = filename, stringsAsFactors=FALSE)
}

