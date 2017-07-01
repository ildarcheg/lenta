require(lubridate)
require(rvest)

baseURL <- "https://lenta.ru"

# setting working directory for mac and win
if (Sys.getenv("HOMEPATH") == "") {
  workingDirectory <- ("~/lenta")
} else {
  workingDirectory <- paste0(Sys.getenv("HOMEPATH"), "\\lenta")  
}
setwd(workingDirectory)


## STEP 1 CODE
# donloading list of pages with archived articles
GetNewsListForPeriod <- function(startDate, endDate) {
  dayArray <- seq(as.Date(startDate), as.Date(endDate), by="days")
  archiveLinkList <- paste0(baseURL, "/", year(dayArray), "/", 
                            formatC(month(dayArray), width = 2, format = "d", 
                            flag = "0"), "/", formatC(day(dayArray), 
                            width = 2, format = "d", flag = "0"), "/")
  newsList <- c()
  for (i in 1:length(archiveLinkList)) {
    print(archiveLinkList[i])
    print(i)
    pg <- read_html(archiveLinkList[i], encoding = "UTF-8")
    total <- html_nodes(pg, xpath=".//section[@class='b-longgrid-column']//div[@class='titles']//a") %>% html_attr("href")   
    newsList <- c(newsList, total)
 
    saveRDS(newsList, file = "data/tempNewsList.rds")
  }
  return(newsList)
}

# getting links to all articles for 2010-2017
Step1 <- function() {
  newsList <- GetNewsListForPeriod(as.Date("2010-01-01"), as.Date("2017-06-30"))
  newsLinkList <- paste0(baseURL, newsList)
  saveRDS(newsLinkList, file = "data/tempNewsLinkList.rds") 
}

## STEP 2 CODE
# Prepare wget code for pages downloading
GetWgetCode <- function() {
  
}

#Step1()
