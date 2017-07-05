GetAndWriteDubHTML <- function(address, num) {
    con <- url(address,  method = c("libcurl"))
    htmlCode = readLines(con)
    close(con)
    writeLines(htmlCode, con = paste("dub/", "t", num+1, ".html", sep = ""), sep = "\n", useBytes = FALSE)
    
}

GetDubHTML <- function(address) {
    temp_file <- "temp1.html"
    con <- url(address,  method = c("libcurl"))
    htmlCode = readLines(con)
    close(con)
    writeLines(htmlCode, con = temp_file, sep = "\n", useBytes = FALSE)
    doc <- htmlTreeParse(temp_file, useInternalNodes=T)
    return(doc)
}

GetDubURL <- function(doc) {
    node <- getNodeSet(doc,"//div[@class='browse_in_list ']/..//a")
    url = xmlSApply(node, xmlGetAttr, "href") 
    url <- unique(url)
    url <- url[order(url)]
    return(url)
}

GetMainUrl <- function() {
    
    address = "https://uae.dubizzle.com/motors/used-cars/"
    doc <- GetDubHTML(address)
    url <- GetDubURL(doc)
    urlmain <- character(0)
    
    length1 = length(url)
    for (i in 1:length1) {
        if (!url[i] == "#") {
            print(url[i])
            address2 <- paste(address, url[i], sep = "")
            doc2 <- GetDubHTML(address2)
            url2 <- GetDubURL(doc2)    
            length2 = length(url2)
            for (k in 1:length2) {
                if (!url2[k] == "#") {
                    address3 <- paste(address2, url2[k], sep = "")
                    urlmain <- c(urlmain, address3)
                    writeLines(urlmain, con = "tt.txt", useBytes = FALSE)
                }
            }        
        }
    }
    return(urlmain)
}

GetDubMaxPages <- function(address) {
    doc <- GetDubHTML(address)
    node <- getNodeSet(doc,"//a[@id='last_page']")
    if (length(node) == 0) {
        maxPages <- 1
    } else {
        
        url = xmlSApply(node, xmlGetAttr, "href")
        url <- unique(url)
        maxPages <- as.numeric(strsplit(url, '=')[[1]][2])
        
    }
    return(maxPages)
}

GetDubCarUrl <- function(pageAddress) {
    pageDoc <- GetDubHTML(pageAddress)
    node <- getNodeSet(pageDoc,"//h3[@id='title']/..//a")
    url = xmlSApply(node, xmlGetAttr, "href")
    url <- as.character(url)
    url <- url[grepl("motors/used-cars/",url)]
    if (length(url) == 0) {
        return(url)
    }
    
    for (k in 1:length(url)) {
        url[k] <- strsplit(url[k], "??back=")[[1]][1]
    }
    url <- unique(url)
    url   
}

GetAndWriteCarInfo <- function(carPageAddress, rowN1) {
    
    print("       getting and writing html")
    pageDoc <- GetAndWriteDubHTML(carPageAddress, rowN1)
    
}

GetCarInfo <- function(carPageAddress, rowN1) {
    
    rowN2 <- rowN1
    print("       getting html")
    pageDoc <- GetDubHTML(carPageAddress)
    print("       starting parsing")
    node <- getNodeSet(pageDoc,"//span[@itemprop='price']")
    price <- as.numeric(gsub(",","", xmlValue(node[[1]])))  
    node <- getNodeSet(pageDoc,"//ul[@class='important-fields']/..//li")
    print("       starting work")
    carInfo <- xmlSApply(node, xmlValue)
    carInfo <- gsub(pattern = "\n", replacement = " ", x = carInfo)
    carInfo <- gsub(pattern = " ", replacement = "", x = carInfo)
    carInfo <- c(paste("Price", price, sep = ":"), carInfo)
    carInfo <- c(paste("rowN", rowN2, sep = ":"), carInfo)
    carInfo <- tbl_dt(carInfo, data.frame)
    carInfo <- mutate(carInfo, mark="", value="")
    for (i in 1:nrow(carInfo)) {
        splittedValue <- strsplit(carInfo$data[i], ":")
        carInfo$mark[i] <- splittedValue[[1]][1]
        carInfo$value[i] <- splittedValue[[1]][2]
    }
    carInfo <- select(carInfo, mark, value)
    print("       finished work")
    carInfo
}

GetCarInfoFromHTML <- function() {
    library(XML)
    library(dplyr)    
    library(lubridate)
    files <- list.files("dub2", "*.html", full.names=T)
    firstT <- TRUE
    listCarInfo <- list()
    num <- length(files)
    pb <- txtProgressBar(min = 0, max = num, style = 3)
    for (i in 1:num) {
        setTxtProgressBar(pb, i)
        doc <- htmlTreeParse(files[i], useInternalNodes=T)    
        node <- getNodeSet(doc,"//span[@itemprop='price']")
        price <- as.numeric(gsub(",","", xmlValue(node[[1]])))  
        node <- getNodeSet(doc,"//h3[@class='listing-details-header']/span")
        dataRaw <- xmlValue(node[[1]])
        dataRaw <- gsub("Posted on: ","", dataRaw)
        data <- dmy(dataRaw)
        node <- getNodeSet(doc,"//span[@id='city-name-dropdown-trigger']")
        cityRaw <- xmlValue(node[[1]])
        cityRaw <- gsub("\r","", cityRaw)
        cityRaw <- gsub("\n","", cityRaw)
        cityRaw <- gsub(" ","", cityRaw)
        
        node <- getNodeSet(doc,"//ul[@class='important-fields']/..//li")
        carInfo <- xmlSApply(node, xmlValue)
        carInfo <- gsub(pattern = "\n", replacement = " ", x = carInfo)
        carInfo <- gsub(pattern = "\r", replacement = " ", x = carInfo)
        carInfo <- gsub(pattern = " ", replacement = "", x = carInfo)
        carInfo <- c(paste("City", cityRaw, sep = ":"), carInfo)
        carInfo <- c(paste("Data", data, sep = ":"), carInfo)
        carInfo <- c(paste("Price", price, sep = ":"), carInfo)
        #carInfo <- c(paste("rowN", i, sep = ":"), carInfo)
        carInfo <- tbl_dt(carInfo, data.frame)
        carInfo <- mutate(carInfo, mark="", value="")
        for (k in 1:nrow(carInfo)) {
            splittedValue <- strsplit(carInfo$data[k], ":")
            carInfo$mark[k] <- splittedValue[[1]][1]
            carInfo$value[k] <- splittedValue[[1]][2]
        }
        carInfo <- select(carInfo, mark, value)
        listCarInfo[[i]] <- carInfo
    }
    
    listCarInfo
   
}

GetInfoByModel <- function(address, rowN) {
    
    firstU <- TRUE
    #address <- "https://dubai.dubizzle.com/motors/used-cars/toyota/corolla/"
    #address <- "https://dubai.dubizzle.com/motors/used-cars/wiesmann/mf5/"
    maxPages <- GetDubMaxPages(address)
    namePage <- "?page="
    print(address)
    print(paste("   max:",maxPages))
    rowN1 <- rowN
    for (i in 1:maxPages) {
        pageAddress <- paste(address, namePage, i, sep = "")
        print(paste("   page:", i, " ", pageAddress))
        urlCars <- GetDubCarUrl(pageAddress)
        if (length(urlCars) <= 1) {
            next()
        }
        for (k in 2:length(urlCars)) { ## length(urlCars)
            carPageAddress <- urlCars[k]   
            print(paste("     link# ", k, " ", carPageAddress, sep = ""))
            GetAndWriteCarInfo(carPageAddress, rowN1)
            #carInfo1 <- GetCarInfo(carPageAddress, rowN1)
            #if (firstU) {
            #        carInfo <- carInfo1
            #        firstU <- FALSE
            #} else {
            #        carInfo <- rbind(carInfo, carInfo1)
            #}
            #write.csv(carInfo, file = paste("temp/", "tt", rowN1, ".csv", sep = ""))
            rowN1 <- rowN1 + 1
        }
    }
    TRUE       
}

ReadDub <- function() {
    
    library(XML)
    library(dplyr)
    carInfo <- logical()
    firstU <- TRUE
    #url <- GetMainUrl()
    url <- readLines("tt.txt")
    rowN <- 1000000
    for (linkN in 1:length(url)) {  ##length(url)
        
        carInfo1 <- GetInfoByModel(url[linkN], rowN)   
        carInfo <- carInfo + carInfo1
        #if (firstU) {
        #        carInfo <- carInfo1
        #        firstU <- FALSE
        ##} else {
        #        carInfo <- rbind(carInfo, carInfo1)
        #}
        #write.csv(carInfo, file = paste("temp/", "tt_m", rowN, ".csv", sep = ""))
        rowN <- rowN + 1000
    }
    carInfo
}