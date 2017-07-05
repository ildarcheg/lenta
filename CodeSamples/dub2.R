GetDubHTML <- function(address) {
    temp_file <- "temp1.html"
    ##con <- url(address,  method = c("libcurl"))
    ##htmlCode = readLines(con)
    htmlCode = readLines(con = address)
    ##close(con)
    writeLines(htmlCode, con = temp_file, sep = "\n", useBytes = FALSE)
    rm(htmlCode)
    doc <- htmlTreeParse(temp_file, useInternalNodes=T)
    return(doc)
}

GetDubMaxPages <- function(address) {
    doc <- GetDubHTML(address)
    node <- getNodeSet(doc,"//a[@id='last_page']")
    rm(doc)
    if (length(node) == 0) {
        maxPages <- 1
    } else {
        
        url = xmlSApply(node, xmlGetAttr, "href")
        url <- unique(url)
        maxPages <- as.numeric(strsplit(url, '=')[[1]][2])
        
    }
    rm(node)
    return(maxPages)
}

GetDubCarUrl <- function(pageAddress) {
    pageDoc <- GetDubHTML(pageAddress)
    node <- getNodeSet(pageDoc,"//h3[@id='title']/..//a")
    rm(pageDoc)
    url = xmlSApply(node, xmlGetAttr, "href")
    rm(node)
    url <- as.character(url)
    url <- url[grepl("motors/used-cars/",url)]
    if (length(url) == 0) {
        return(url)
    }
    
    for (k in 1:length(url)) {
        url[k] <- strsplit(url[k], "?back=")[[1]][1]
    }
    url <- unique(url)
    return(url)   
}

GetInfoByModel <- function(address) {
    maxPages <- GetDubMaxPages(address)
    namePage <- "?page="
    print(address)
    print(paste("   max:",maxPages))
    carPageAddress <- character()
    
    for (i in 1:maxPages) {
        pageAddress <- paste(address, namePage, i, sep = "")
        print(paste("   page:", i, " ", pageAddress))
        urlCars <- GetDubCarUrl(pageAddress)
        if (length(urlCars) <= 1) {
            next()
        }
        carPageAddress <- c(carPageAddress, urlCars[2:length(urlCars)])
    }
    return(carPageAddress)       
}

GetDubUrls <- function() {
    library(XML)
    library(dplyr)
    url <- readLines("tt.txt")
    carUrls <- character()
    num <- length(url)
    pb <- txtProgressBar(min = 0, max = num, style = 3)
    for (linkN in 1:num) { 
        setTxtProgressBar(pb, linkN)
        temp <- GetInfoByModel(url[linkN])
        carUrls <- c(carUrls, temp)
        writeLines(carUrls, con = "ttt.txt")
    }
}

GetAndWriteDubHTML <- function(address, part, num) {
    con <- url(address,  method = c("libcurl"))
    htmlCode = readLines(con)
    close(con)
    tt <- gsub("-", "", as.character.Date(Sys.time()))
    tt <- gsub(":", "", tt)
    tt <- gsub(" ", "", tt)
    writeLines(htmlCode, con = paste("dub2/", part, "/", "t-", part, "-", num, "-", tt, ".html", sep = ""), sep = "\n", useBytes = FALSE)
    rm(htmlCode)
    return(list(message = "OK"))
}

GetHTMLs <- function() {
    urls <- readLines(con = "ttt.txt")
    for (i in 1:length(urls)) {
        GetAndWriteDubHTML(urls[i], 1, i)    
    }
}

GetHTMLsByPart <- function(currentNum) {
    
    path <- paste("dub2/log", currentNum, ".txt", sep = "")
    urls <- readLines(con = path)

    urlsLeft <- character()

    for (i in 1:length(urls)) {
        if (is.na(urls[i])) {
            next()
        }        
        res <- tryCatch(GetAndWriteDubHTML(urls[i], currentNum, i), error = function(x) x)  
        
        if (res$message == "OK") {
            
            urlsForWrite <- c(urlsLeft, urls[(i+1):length(urls)])
            writeLines(urlsForWrite, con = path, sep = "\n", useBytes = FALSE)
            
        } else {
            
            urlsLeft <- c(urlsLeft, urls[i])
            urlsForWrite <- c(urlsLeft, urls[(i+1):length(urls)])
            writeLines(urlsForWrite, con = path, sep = "\n", useBytes = FALSE)
            
        }
    }
}

GetLog <- function() {
    
    cc <- character()
    files <- list.files("dub2", "*.txt", full.names=T)
    for (i in 1:length(files)) {
        ccc <- readLines(con = files[i])
        cc <- c(cc, ccc)
    }
    cc <- cc[!cc == "Transferred a partial file"]
    cc
}

CreateHTML <- function() {
    
    ##urls <- readLines(con = "dub2/log.txt")
    urls <- readLines(con = "ttt.txt")
    x <- character()
    x <- c(x, "<html>")
    x <- c(x, "   <body>")
    for (i in 1:length(urls)) {
        x <- c(x, "   <br>")
        xx <- paste("      <a href=", urls[i], ">file-", i, ".dat</a>", sep = "")
        x <- c(x, xx)
    }
    x <- c(x, "   </body>")
    x <- c(x, "</html>")
    writeLines(x, con = paste("dub2/", "linkAll.html", sep = ""), sep = "\n", useBytes = FALSE)
}

ReadAndWriteDubHTMLLoop <- function(address, part, num) {
    con <- url(address,  method = c("libcurl"))
    htmlCode = readLines(con)
    close(con)
    tt <- gsub("-", "", as.character.Date(Sys.time()))
    tt <- gsub(":", "", tt)
    tt <- gsub(" ", "", tt)
    writeLines(htmlCode, con = paste("dub2/", "t-", part, "-", num, "-", tt, ".html", sep = ""), sep = "\n", useBytes = FALSE)
    rm(htmlCode)
    return(list(message = "OK"))
}

ReadHTMLLoop <- function(al) {
 
    currentNum <- al$num
    print(currentNum)
    urls <- al$urls
    
    for (i in 1:length(urls)) {
        if (is.na(urls[i])) {
            next()
        }        
        
        res <- tryCatch(ReadAndWriteDubHTMLLoop(urls[i], currentNum, i), error = function(x) x)  
    }   
}

ReadInLoop <- function() {
    
    library(XML)
    library(dplyr)
    library(parallel)
    ReadHTMLLoop <- function(al) {
        
        currentNum <- al$num
        print(currentNum)
        urls <- al$urls
        
        for (i in 1:length(urls)) {
            if (is.na(urls[i])) {
                next()
            }        
            
            res <- tryCatch(ReadAndWriteDubHTMLLoop(urls[i], currentNum, i), error = function(x) x)  
        }   
    }
    urlsM <- readLines(con = "ttt.txt")
    l <- list()
    for (currentNum in 1:8) {
        nPart = 5000
        urls <- urlsM[(nPart * currentNum - (nPart - 1)):(nPart * currentNum)] 
        al <- list(num = currentNum, urls = urls)
        l[[currentNum]] <- al
    }
    cl <- makeCluster(8)
    parLapply(cl, l, function(z) {ReadHTMLLoop(z)})
    stopCluster(cl)
}

LoopFun <- function(myX) {
    print("part")
    print(class(myX))
    print(myX)
}

LoopTest <- function() {
    l <- list()
    l[[1]] <- seq(1,20)
    l[[2]] <- seq(5,20)
    l[[3]] <- seq(10,20)
    l[[4]] <- seq(15,20)
    
    parallel::mclapply(l, LoopFun)
    
}