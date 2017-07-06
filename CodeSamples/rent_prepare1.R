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

GetDF <- function() {
    
    df <- data.frame(city = character(), price__gte = integer(), price__lte = integer(), 
                     bedrooms__gte = integer(), bedrooms__lte = integer(), 
                     size__gte = integer(), size__lte = integer(), url = character(), maxPages = integer(), use = logical(), stringsAsFactors = FALSE)
    df[7*13, ]$city <- ""
    df$city <- rep(c("abudhabi", "ajman", "alain", "fujairah", "rak", "sharjah", "uaq"), 13)
    df$price__gte <- rep(0, 7*13)
    df$price__lte <- rep(10000000, 7*13)
    df$bedrooms__gte <- rep(0:12, each = 7)
    df$bedrooms__lte <- rep(0:12, each = 7)
    df$size__gte <- rep(0, 7*13)
    df$size__lte <- rep(20000, 7*13)
    df$url <- rep("", 7*13)
    df$maxPages <- rep(0, 7*13)
    df 
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

SeparateBySize <- function(dt) {

    dtnrow <- nrow(dt)
    
    for (i in dtnrow:1) {
        
        if (dt[i, ]$use == FALSE) {
            next()
        }

        if (i > 1) {
            dt1 <- dt[1:(i-1), ]
        } else { 
            dt1 <- dt[0, ]
        }        
        size__gte <- dt[i, ]$size__gte
        size__lte <- dt[i, ]$size__lte
        meanSize <- as.integer(mean(c(size__gte, size__lte)))
        
        dt21 <- dt[i, ]
        
        dt21[1, ]$size__gte <- size__gte 
        dt21[1, ]$size__lte <- meanSize -1
        dt21[1, ]$maxPages <- 0
        dt21[1, ]$url <- ""
        dt22 <- dt[i, ]
        dt22[1, ]$size__gte <- meanSize
        dt22[1, ]$size__lte <- size__lte        
        dt22[1, ]$maxPages <- 0    
        dt22[1, ]$url <- ""
        
        if ((i+1) <= nrow(dt)) {
            dt3 <- dt[(i+1):nrow(dt), ]
        } else { 
            dt3 <- dt[0, ]
        }
        dt <- rbind_all(list(dt1, dt21, dt22, dt3))
    }
    dt
}

SeparateByPrice <- function(dt) {
    
    dtnrow <- nrow(dt)
    
    for (i in dtnrow:1) {
        
        if (dt[i, ]$use == FALSE) {
            next()
        }
        
        if (i > 1) {
            dt1 <- dt[1:(i-1), ]
        } else { 
            dt1 <- dt[0, ]
        }        
        price__gte <- dt[i, ]$price__gte
        price__lte <- dt[i, ]$price__lte
        meanPrice <- as.integer(mean(c(price__gte, price__lte)))
        
        dt21 <- dt[i, ]
        
        dt21[1, ]$price__gte <- price__gte 
        dt21[1, ]$price__lte <- meanPrice -1
        dt21[1, ]$maxPages <- 0
        dt21[1, ]$url <- ""
        dt22 <- dt[i, ]
        dt22[1, ]$price__gte <- meanPrice
        dt22[1, ]$price__lte <- price__lte        
        dt22[1, ]$maxPages <- 0    
        dt22[1, ]$url <- ""
        
        if ((i+1) <= nrow(dt)) {
            dt3 <- dt[(i+1):nrow(dt), ]
        } else { 
            dt3 <- dt[0, ]
        }
        dt <- rbind_all(list(dt1, dt21, dt22, dt3))
    }
    dt
}

PrepareDf <- function() {
    
    df <- GetDF()
    dfnRow <- nrow(df)
    
    for (i in 1:dfnRow) {
        url <- GetFilterUrl(as.list(df[i, ]))
        maxPages <- GetDubMaxPages(url)
        df[i, ]$url <- url
        df[i, ]$maxPages <- maxPages
    }    
    df$use <- df$maxPages == 40
    saveRDS(df, "data.rds")
    df
}

SetMaxPages <- function(dt) {
    
    dtnRow <- nrow(dt)
    for (i in 1:dtnRow) {
        
        if (dt[i, ]$use == FALSE) {
            next()
        }
        
        url <- GetFilterUrl(as.list(dt[i, ]))
        maxPages <- GetDubMaxPages(url)
        dt[i, ]$url <- url
        dt[i, ]$maxPages <- maxPages
        dt[i, ]$use <- maxPages == 40
    }
    
    dt    
    
}

ProcessDt <- function(n, dt) {
    for (i in 1:n) {
        print(paste0("2^",i,", by ", 20000/2^i))
        dt <- SeparateBySize(dt)
        dt <- SetMaxPages(dt) 
        df <- as.data.frame(dt)
        saveRDS(df, paste0("data", i, ".rds"))
    }
    dt
}

ProcessDt2 <- function(n, dt) {
    for (i in 1:n) {
        print(paste0("2^",i,", by ", 10000000/2^i))
        dt <- SeparateByPrice(dt)
        dt <- SetMaxPages(dt) 
        df <- as.data.frame(dt)
        saveRDS(df, paste0("data", i, ".rds"))
    }
    dt
}

Test <- function() {
    
    df <- readRDS("data.rds")
    dt <- as.tbl(df)
    dt$use <- dt$maxPages == 40
       
    dt <- ProcessDt(9, dt)
    
    dt
}

Test2 <- function() {
    
    df <- readRDS("data.rds")
    dt <- as.tbl(df)
    dt <- ProcessDt2(14, dt)
    
    dt
}

Test3 <- function() {
    
    df <- readRDS("data.rds")
    dt <- as.tbl(df)

    dtnrow <- nrow(dt)
    dt <- mutate(dt, page = 0)
    
    for (i in dtnrow:1) {
        
        if (dt[i, ]$maxPages == 1) {
            dt[i, ]$page <- 1
            next()
        }
        
        if (i > 1) {
            dt1 <- dt[1:(i-1), ]
        } else { 
            dt1 <- dt[0, ]
        }        
        
        maxPages <- dt[i, ]$maxPages
        dt2 <- dt[i, ]
        dt2[1, ]$page <- 1
        
        for (k in 2:maxPages) {
            dtt<- dt[i, ]
            dtt[1, ]$page <- k
            dt2 <- rbind_all(list(dt2, dtt))
        }

        
        if ((i+1) <= nrow(dt)) {
            dt3 <- dt[(i+1):nrow(dt), ]
        } else { 
            dt3 <- dt[0, ]
        }
        dt <- rbind_all(list(dt1, dt2, dt3))
    }
    
    dt
}

GerURLs <- function() {
    
    library(XML)
    library(dplyr)    
    httpURL <- "https://"
    mainUTL <- ".dubizzle.com/property-for-rent/residential/apartmentflat/"
    filterURL <-"?price__gte=&price__lte=&bedrooms__gte=&bedrooms__lte=&is_basic_search_widget=0&is_search=1&size__gte=&size__lte="
    
    apart <- seq(0,12)
    cities <- c("abudhabi", "ajman", "alain", "fujairah", "rak", "sharjah", "uaq")
    size <- seq(0, 10000, by = 500)  
    price <- seq(0, 2000000, by = 1000)
    UrlsPages <- character()
    
    for (i in 1:length(cities)) {
        
        ## ALL ADs IN CITY
        url <- paste(httpURL, cities[i], mainUTL, filterURL, sep = "")
        maxPages <- GetDubMaxPages(url)
        print(url)
        print(paste("", maxPages, sep = ""))
    }
    
}

GerURLs2 <- function() {
    library(XML)
    library(dplyr)    
    httpURL <- "https://"
    mainUTL <- ".dubizzle.com/property-for-rent/residential/apartmentflat/"
    filterURL <-"?price__gte=&price__lte=&bedrooms__gte=&bedrooms__lte=&is_basic_search_widget=0&is_search=1&size__gte=&size__lte="
    
    apart <- seq(0,12)
    cities <- c("abudhabi", "ajman", "alain", "fujairah", "rak", "sharjah", "uaq")
    size <- seq(0, 10000, by = 500)  
    price <- seq(0, 2000000, by = 1000)
    UrlsPages <- character()
    
    for (i in 1:length(cities)) {
        
        ## ALL ADs IN CITY
        url <- paste(httpURL, cities[i], mainUTL, filterURL, sep = "")
        maxPages <- GetDubMaxPages(url)
        print(url)
        print(paste("", maxPages, sep = ""))
        if (maxPages < 40) {
            UrlsPages <- c(UrlsPages, url)
        } else {
            print("going deeper")            
            for (QBedroom in 1:length(apart)) {
                
                ## ALL ADs IN CITY/BY BEDROOM
                filterURLNew <- gsub("bedrooms__gte=", paste("bedrooms__gte=", apart[QBedroom], sep = ""), filterURL)
                filterURLNew <- gsub("bedrooms__lte=", paste("bedrooms__lte=", apart[QBedroom], sep = ""), filterURLNew)
                url <- paste(httpURL, cities[i], mainUTL, filterURLNew, sep = "")
                maxPages <- GetDubMaxPages(url)
                print(paste("  ", url, sep = ""))
                print(paste("  ", maxPages, sep = ""))
                if (maxPages < 40) {
                    UrlsPages <- c(UrlsPages, url)
                } else {
                    print("  going deeper")            
                    for (Qsize in 2:length(size)) {
                        ## ALL ADs IN CITY/BY BEDROOM/BY SIZE
                        filterURLNew <- gsub("bedrooms__gte=", paste("bedrooms__gte=", apart[QBedroom], sep = ""), filterURL)
                        filterURLNew <- gsub("bedrooms__lte=", paste("bedrooms__lte=", apart[QBedroom], sep = ""), filterURLNew)                        
                        filterURLNew <- gsub("size__gte=", paste("size__gte=", size[Qsize-1], sep = ""), filterURLNew)
                        filterURLNew <- gsub("size__lte=", paste("size__lte=", size[Qsize]-1, sep = ""), filterURLNew)
                        url <- paste(httpURL, cities[i], mainUTL, filterURLNew, sep = "")
                        maxPages <- GetDubMaxPages(url)
                        print(paste("    ", url, sep = ""))
                        print(paste("    ", maxPages, sep = ""))                        
                        if (maxPages < 40) {
                            UrlsPages <- c(UrlsPages, url)
                        } else {
                            print("    going deeper")            
                            for (Qprice in 2:length(price)) {
                                ## ALL ADs IN CITY/BY BEDROOM/BY SIZE/BY PRICE
                                filterURLNew <- gsub("bedrooms__gte=", paste("bedrooms__gte=", apart[QBedroom], sep = ""), filterURL)
                                filterURLNew <- gsub("bedrooms__lte=", paste("bedrooms__lte=", apart[QBedroom], sep = ""), filterURLNew)                        
                                filterURLNew <- gsub("size__gte=", paste("size__gte=", size[Qsize-1], sep = ""), filterURLNew)
                                filterURLNew <- gsub("size__lte=", paste("size__lte=", size[Qsize]-1, sep = ""), filterURLNew)
                                filterURLNew <- gsub("price__gte=", paste("price__gte=", price[Qprice-1], sep = ""), filterURLNew)
                                filterURLNew <- gsub("price__lte=", paste("price__lte=", price[Qprice]-1, sep = ""), filterURLNew)                                
                                url <- paste(httpURL, cities[i], mainUTL, filterURLNew, sep = "")
                                maxPages <- GetDubMaxPages(url)
                                print(paste("      ", url, sep = ""))
                                print(paste("      ", maxPages, sep = ""))                        
                                
                            }
                        }                        
                    }
                }
                                
            }
        }
    }
}