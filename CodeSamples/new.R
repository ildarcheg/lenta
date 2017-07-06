setwd("C:/Users/igabdrakhmanov/Desktop/rscripts")
> con = url("https://dubai.dubizzle.com/motors/used-cars/audi/")
> htmlCode = readLines(con)
> library(XML)
> writeLines(htmlCode, con = "htmlCode.html", sep = "\n", useBytes = FALSE)
> doc <- htmlTreeParse("htmlCode.html")
> doc <- htmlTreeParse("htmlCode.html", useInternalNodes=T)
> docText <-xpathSApply(doc, "//div[@class='browse_in_list ']", xmlValue)
> writeLines(docText, con = "file1.txt", sep = "\n", useBytes = FALSE)
> doc1 <- htmlTreeParse("file1.txt", useInternalNodes=T)
> 



xml_string = c('<?xml version="1.0" encoding="UTF-8"?>','<movies>','<movie mins="126" lang="eng">','<title>Good Will Hunting</title>','<director>','<first_name>Gus</first_name>','<last_name>Van Sant</last_name>','</director>','<year>1998</year>','<genre>drama</genre>','</movie>','<movie mins="106" lang="spa">','<title>Y tu mama tambien</title>','<director>','<first_name>Alfonso</first_name>','<last_name>Cuaron</last_name>','</director>','<year>200</year>','<genre>drama</genre>','</movie>','</movies>')