require(lubridate)
require(rvest)

if (Sys.getenv("HOMEPATH") == "") {
  workingDirectory <- ("~/lenta")
} else {
  workingDirectory <- paste0(Sys.getenv("HOMEPATH"), "\\lenta")  
}
setwd(workingDirectory)

GetNewsList <- function() {
  dayArray <- seq(as.Date("2010-01-01"), as.Date("2017-06-30"), by="days")
  baseURL <- "https://lenta.ru"
  archiveLinkList <- paste0(baseURL, "/", year(dayArray), "/", 
                            formatC(month(dayArray), width = 2, format = "d", 
                            flag = "0"), "/", formatC(day(dayArray), 
                            width = 2, format = "d", flag = "0"), "/")
  newsList <- c()
  for (i in 1:length(archiveLinkList)) {
    pg <- read_html(archiveLinkList[i], encoding = "UTF-8")
    total <- html_nodes(pg, xpath=".//section[@class='b-longgrid-column']//div[@class='titles']//a") %>% html_attr("href")   
    newsList <- c(newsList, total)
    print(i)
    saveRDS(newsList, file = "data/tempNewsList.rds")
  }
  return(newsList)
}

newsList <- GetNewsList()
newsLinkList <- paste0(baseURL, newsList)
saveRDS(newsLinkList, file = "data/tempNewsLinkList.rds")