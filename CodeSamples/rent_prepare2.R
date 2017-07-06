GetDubHTML <- function(address) {
    temp_file <- "temp1.html"
    con <- url(address,  method = c("libcurl"))
    htmlCode = readLines(con)
    close(con)
    writeLines(htmlCode, con = temp_file, sep = "\n", useBytes = FALSE)
    doc <- htmlTreeParse(temp_file, useInternalNodes=T)
    return(doc)
}

GetDubMaxPages <- function(address) {
    doc <- GetDubHTML(address)
    node <- getNodeSet(doc,"//a[@id='last_page']")
    if (length(node) == 0) {
        maxPages <- 1
        nodeNoResult <-getNodeSet(doc,"//h2[@id='no-results']")
        if (!length(nodeNoResult) == 0) {
            maxPages <- 0
        }
        
    } else {
        
        url = xmlSApply(node, xmlGetAttr, "href")
        url <- unique(url)
        temp <- strsplit(url, '=')[[1]][2]
        temp <- strsplit(temp, '&')[[1]][1]
        maxPages <- as.numeric(temp)
    }
    return(maxPages)
}

GetFilterUrl <- function(filterList) {
    
    ##https://abudhabi.dubizzle.com/property-for-rent/residential/apartmentflat/?price__gte=100&price__lte=1000000&bedrooms__gte=0&bedrooms__lte=12&size__gte=2010&size__lte=2050&bathrooms__gte=&bathrooms__lte=
    ##https://abudhabi.dubizzle.com/property-for-rent/residential/apartmentflat/?site=3&s=RP&rc=1743&c1=24&price__gte=100&price__lte=1000000&bedrooms__gte=0&bedrooms__lte=12&places__id__in=--&building__in=&keywords=&is_basic_search_widget=0&is_search=1&added__gte=&company_item_id=&furnished=&listed_by=&size__gte=2010&size__lte=2050&bathrooms__gte=&bathrooms__lte=&real_estate_agent=&landlord=&property_developer=
    httpURL <- "https://"
    mainUTL <- ".dubizzle.com/property-for-rent/residential/apartmentflat/"
    city <- filterList$city
    filterNames <- names(filterList)
    url <- paste(httpURL, city, mainUTL, "?", sep = "")
    urlFilter <- paste(filterNames[2], "=", format(filterList[filterNames[2]], scientific = FALSE), "", sep = "")
    for (i in 3:(length(filterNames)-3)) {
        urlFilter1 <- paste(filterNames[i], "=", format(filterList[filterNames[i]], scientific = FALSE), "", sep = "")
        urlFilter <- paste(urlFilter, urlFilter1, sep = "&")
    }
    url <- paste(url, urlFilter, sep = "")
    url
}

GetDubCarUrl <- function(pageAddress) {
    pageDoc <- GetDubHTML(pageAddress)
    node <- getNodeSet(pageDoc,"//h3[@id='title']/..//a")
    rm(pageDoc)
    url = xmlSApply(node, xmlGetAttr, "href")
    rm(node)
    url <- as.character(url)
    url <- url[grepl("dubizzle.com/en/property-for-rent",url)]
    if (length(url) == 0) {
        return(url)
    }
    
    for (k in 1:length(url)) {
        url[k] <- strsplit(url[k], "?back=")[[1]][1]
    }
    url <- unique(url)
    return(url)   
}

Test <- function() {
    
    df <- readRDS("data.rds")
    dt <- as.tbl(df)
    
    dtnRow <- nrow(dt)
    for (i in 1:dtnRow) {
        url <- GetFilterUrl(as.list(dt[i, ]))
        dt[i, ]$url <- paste0(url, "&page=", dt[i, ]$page)
    }
    dt
}

TestByNum <- function(num) {
    
    library(XML)
    library(dplyr)
    
    df <- readRDS("data.rds")
    dt <- as.tbl(df)
    
    dtnRow <- nrow(dt)
    st <- 1 +100*(num-1)
    ft <- min(st + 99, dtnRow)
    
    rentPageAddress <- character()
    
    for (i in st:ft) {
        dt[i, ]$url <- ""
        url <- GetFilterUrl(as.list(dt[i, ]))
        dt[i, ]$url <- paste0(url, "&page=", dt[i, ]$page)
    }
    for (i in st:ft) {
        pageAddress <- dt[i, ]$url
        print(paste0("i: ", i, " ", pageAddress))
        urls <- GetDubCarUrl(pageAddress)
        if (length(urls) <= 1) {
            next()
        }
        rentPageAddress <- c(rentPageAddress, urls[2:length(urls)])        
    }
    
    saveRDS(rentPageAddress, paste0("lll", num, ".rds"))
    print("done")
}

PrepPagesList <- function() {
    
    library(XML)
    library(dplyr)    
    library(lubridate)
    files <- list.files(pattern = "lll*", full.names=T)
    num <- length(files)
    vec <- character()
    
    for (i in 1:num) {
        x <- readRDS(files[i])    
        vec <- c(vec, as.vector(x))
    }
    vec
}