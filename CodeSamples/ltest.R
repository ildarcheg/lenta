GetPageText <- function(address) {
        
        webpage <- getURL(address, followLocation = TRUE, .opts = list(timeout = 10))
        pagetree <- htmlTreeParse(webpage, error = function(...) {}, useInternalNodes = TRUE, encoding = "UTF-8")
        node <- getNodeSet(pagetree, "//div[@itemprop='articleBody']/..//p")
        plantext <- xmlSApply(node, xmlValue)
        plantext <- paste(plantext, collapse = "")
        node <- getNodeSet(pagetree, "//title")
        title <- xmlSApply(node, xmlValue)
        
        return(list(plantext = plantext, title = title))
}

DownloadPlanText <- function() {
        
        tempUrls <- c("https://lenta.ru/news/2009/12/31/kids/",
                      "https://lenta.ru/news/2009/12/31/silvio/",
                      "https://lenta.ru/news/2009/12/31/postpone/",
                      "https://lenta.ru/news/2009/12/31/boeviks/",
                      "https://lenta.ru/news/2010/01/01/celebrate/",
                      "https://lenta.ru/news/2010/01/01/aes/")
        
        for (i in 1:length(tempUrls)) {
                print(system.time(GetPageText(tempUrls[i])))
        }
}