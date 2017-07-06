ParseDownloadedFiles <- function() {
  
  dataFolder <- file.path(getwd(), "data")
  folders <- list.files(dataFolder, full.names = FALSE, recursive = FALSE, pattern = "-")
  for (i in 1:length(folders)) {
    folderName <- folders[i]
    currentFolder <- file.path(dataFolder, folderName) 
    files <- list.files(currentFolder, full.names = TRUE, recursive = FALSE, pattern = "index")  
    
  }
  
  files <- list.files(file.path(getwd(), "data"), full.names = TRUE, recursive = TRUE, pattern = "index")
  files <- files[1:800]
  
  numberOfFiles <- length(files)
  print(numberOfFiles)
  groupSize <- 1000
  filesGroup <- seq(from = 1, to = numberOfFiles, by = groupSize)
  dfList <- list()
  for (i in 1:length(filesGroup)) {
    firstFileInGroup <- filesGroup[i]
    lastFileInGroup <- min(firstFileInGroup + groupSize - 1, numberOfFiles)
    dfList[[i]] <- map_df(files[firstFileInGroup:lastFileInGroup], ReadFile)
  }
  dfList
}

## STEP 5 CODE
# Combine downloaded files
GetCMDFilesToCombine <- function() {
  
  dataFolder <- file.path(getwd(), "data")
  folders <- list.files(dataFolder, full.names = FALSE, recursive = FALSE, pattern = "-")
  cmdFile <- c()
  for (i in 1:length(folders)) {
    folderName <- folders[i]
    currentFolder <- file.path(dataFolder, folderName) 
    files <- list.files(currentFolder, full.names = TRUE, recursive = FALSE, pattern = "index")
    fileName <- file.path(dataFolder, paste0("combine/filesInFolder", folderName, ".list"))
    writeLines(files, fileName)
    #cat filesInFolder240001-260000.list | xargs -n 32 -P 8 cat >> /Users/ildar/lenta/data/000000.ht
    fileNameCombine <- file.path(dataFolder, paste0("combine/", folderName, ".combine"))
    cmdFile <- c(cmdFile, paste0("cat ",fileName, " | xargs -n 32 -P 8 cat >> ", fileNameCombine))
  }
  writeLines(cmdFile, file.path(dataFolder, "cmd.run"))
}

## STEP 6 CODE
# Parsing
ReadCombinedFile <- function(filename) {
  
  dataFolder <- file.path(getwd(), "data")
  combinedFolder <- file.path(dataFolder, "combine")
  files <- list.files(combinedFolder, full.names = TRUE, recursive = FALSE, pattern = ".combine")
  
  filename <- files[1]
  res <- system.time(pagetree <- htmlTreeParse(filename, error = function(...) {}, useInternalNodes = TRUE, encoding = "UTF-8"))
  res
  node <- getNodeSet(pagetree, "//head/link[@rel='canonical']")
  
  nodesURL <- getNodeSet(pagetree, "//head/link[@rel='canonical']")
  url <- xmlSApply(node, xmlGetAttr, "href")
  
  
  articleBodyNode <- html_nodes(pg, xpath=".//div[@itemprop='articleBody']")
  plaintext <- html_nodes(articleBodyNode, xpath=".//p") %>% html_text() %>% paste0(collapse="") 
  plaintextLinks <- html_nodes(articleBodyNode, xpath=".//a") %>% html_attr("href") %>% unique() %>% paste0(collapse=" ")
  
  additionalLinks <- html_nodes(pg, xpath=".//section/div[@class='item']/div/..//a") %>% html_attr("href") %>% unique() %>% paste0(collapse=" ")
  
  title <- html_text(html_nodes(pg, xpath=".//head/title"))
  imageNodes <- html_nodes(pg, xpath=".//div[@class='b-topic__title-image']")
  imageDescription <- html_nodes(imageNodes, xpath="div/div[@class='b-label__caption']") %>% html_text()
  imageCredits <- html_nodes(imageNodes, xpath="div/div[@class='b-label__credits']") %>% html_text()
  videoNodes <- html_nodes(pg, xpath=".//div[@class='b-video-box__info']")
  videoDescription <- html_nodes(videoNodes, xpath="div[@class='b-video-box__caption']") %>% html_text()
  videoCredits <- html_nodes(videoNodes, xpath="div[@class='b-video-box__credits']") %>% html_text()  
  
  url <- html_attr(html_nodes(pg, xpath=".//head/link[@rel='canonical']"), "href")
  author <- html_text(html_nodes(pg, xpath=".//span[@class='name']"))
  authorLength <- length(author)
  if (authorLength==0) {author <- ""}
  datetime <- html_nodes(pg, xpath=".//time[@class='g-date']") %>% html_attr("datetime")
  datetime <- datetime[!is.na(datetime)][1]
  datetimeLength <- length(datetime)
  if (datetimeLength==0) {datetime <- NA}
  
  ##print(paste("Part ", filename))
  data.frame(url = url, title = title, plaintext = plaintext, author = author, datetime = datetime, stringsAsFactors=FALSE)
  
}
