
GetCarInfoFromHTML <- function() {
    library(XML)
    library(dplyr)    
    library(lubridate)
    files <- list.files("dub2/New folder", "*.html", full.names=T)
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

ReadMainCSV <- function() {
    library(dplyr)
    df <- read.csv("main.csv")
    dt <- tbl_df(df)
    dt$mark <- as.character(dt$mark)
    dt$value <- as.character(dt$value)
    dt <- select(dt, mark, value)
    rowPrice <- dt$mark == "Price"
    sumRowPrice <- sum(rowPrice)
    newSeq <- seq(1:sumRowPrice)
    #print(rowPrice[1:75])
    num <- nrow(dt)
    pos <- which(rowPrice)
    pos <- c(pos, nrow(dt)+1)
    l <- list()
    for (i in 2:length(pos)) {
        l[[i-1]] <- c(pos[i-1], pos[i]-1)
        #         print(pos[i-1])
        #         print(pos[i]-1)
        #         print("---")
    }
    mutate(dt, id = "")
    newL <- list()
    for (i in 1:length(l)) {
        xx <- dt[c(l[[i]][1]:l[[i]][2]), ]
        idTag <- xx$value[xx$mark == "id"]
        xx$id <- idTag
        xx <- filter(xx, !mark == "id")
        newL[[i]] <- xx
    }
    dt <- rbind_all(newL)
    dt
}