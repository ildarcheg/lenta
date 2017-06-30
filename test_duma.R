require(rvest)
require(rjson)
require(httr)
require(dplyr)
require(data.table)
require(lubridate)

getVoteHistory <- function() {
  
  voteHistory <- c()
  baseURL <- "http://vote.duma.gov.ru/?convocation=AAAAAAA"
  additionalURL <- "&sort=date_desc&page="
  for (covoc in 1:7) {
    firstVoteHistoryURL <- paste0(baseURL, covoc, additionalURL, "1")
    pg <- read_html(firstVoteHistoryURL, encoding = "UTF-8")
    total <- html_nodes(pg, xpath=".//a[@class='delta-0']") %>% html_text() %>% as.numeric() 
    for (i in 1:total) {
      voteHistoryURL <- paste0(baseURL, covoc, additionalURL, i)
      voteHistory <- c(voteHistory, voteHistoryURL)
    }
  }
  saveRDS(voteHistory, "generalVotePageList.rds")
}

getVoteLinksList <- function() {
  
  voteHistory <- readRDS("generalVotePageList.rds")  
  baseURL <- "http://vote.duma.gov.ru"
  voteLinksList <- c()
  for (i in 1:length(voteHistory)) {
    print(i)
    voteHistoryURL <- voteHistory[i]  
    pg <- read_html(voteHistoryURL, encoding = "UTF-8")
    votes <- html_nodes(pg, xpath=".//div[@class='item-left']/..//a") %>% html_attr("href")
    for (k in 1:length(votes)) {
      voteLink <- paste0(baseURL, votes[k])
      voteLinksList <- c(voteLinksList, voteLink)        
    }
    saveRDS(voteLinksList, "generalVoteList.rds")
  } 
  saveRDS(voteLinksList, "generalVoteList.rds")
}

downloadVotesResults <- function() {
  t <- readRDS("temp.rds")
  if (!file.exists(file.path("t"))) {
    dir.create(file.path("t"))
  }        
  for (i in 1:length(t)) {
    link <- t[i]
    pg <- read_html(link, encoding = "UTF-8")
    saveRDS(pg, paste0("t/", i, ".rds"))
    #writeLines(pg, paste0("t/", i, ".html"))
  }
}

readVoteResult <- function(){
  
  df <- data.frame(convocation = character(),
                   depName= character(), 
                   depID = character(),
                   depFaction = character(), 
                   depfactionID = character(), 
                   depIsCurrent = logical(), 
                   depVoteResult = character(), 
                   voteDate = character(),
                   voteDateAPI = character(),
                   voteID = character(), 
                   voteSubject = character(), 
                   voteLawNumber = character(),
                   voteAsozdLink = character(),
                   voteFor = character(), 
                   voteAgainst = character(),
                   voteAbstain = character(),
                   voteAbsent = character(),
                   voteResultType = character(),
                   voteResult = logical(),
                   stringsAsFactors = FALSE)     
  
  files <- list.files("p","*", full.names=T, recursive = TRUE)        
  l <- list()
  nn <- 0
  vD <- c()
  for (i in 1:length(files)) {
    print(i)
    voteFile <- files[i]
    voteID <- gsub("p/", "", voteFile)
    pg <- read_html(voteFile, encoding = "UTF-8")
    datePNode <- html_nodes(pg, xpath=".//div[@class='date-p']")
    voteDate <- html_nodes(datePNode, xpath=".//span") %>% html_text()
    asozdLink <- html_nodes(datePNode, xpath=".//a[contains(text(), 'Страница законопроекта в АСОЗД')]") %>% html_attr("href")
    if (length(asozdLink) == 0) {asozdLink = NA}
    
    scriptsText <- html_nodes(pg, xpath=".//script") %>% html_text()
    scripts <- grep("deputiesData = ", scriptsText)
    #print("part1")
    for (k in scripts) {
      script <- scriptsText[k]
      
      startPosition <- regexpr("deputiesData = ", script)[1]
      endPosition <- regexpr("}];", script)[1]
      
      if (endPosition == -1) {
        vD <- c(vD, i)     
        break
      }
      s <- substr(script, startPosition+15, endPosition+1)
      #print("part2")
      ##print(s)
      sj <- fromJSON(s)
      ll <- list(voteID = voteID, voteDate = voteDate, asozdLink = asozdLink, sj = sj)
      nn <- nn + 1
      l[[nn]] <- ll
      #for (j in 1:length(sj)) {
      
      #depLine <- sj[[j]]
      #url <- depLine$url
      # depFrame <- data.frame(depName = depLine$sortName,
      #                 depFaction = depLine$faction,
      #                 depfactionID = depLine$factionCode,
      #                 depVoteResult = as.character(depLine$result), stringsAsFactors = FALSE)
      #df <- bind_rows(df, depFrame)
      #}
    }
    
    if ((i %% 10000) == 0) {
      saveRDS(l, paste0("l", i,".rds"))        
    }
  }
  saveRDS(l, "l.rds")
  saveRDS(vD, "vD.rds")
}

readVotes <- function(){
  
  files <- list.files("p","*", full.names=T, recursive = TRUE)        
  l <- list()
  nn <- 0
  for (i in 1:length(files)) {
    print(i)
    voteFile <- files[i]
    voteID <- gsub("p/", "", voteFile)
    pg <- read_html(voteFile, encoding = "UTF-8")
    datePNode <- html_nodes(pg, xpath=".//div[@class='date-p']")
    voteDate <- html_nodes(datePNode, xpath=".//span") %>% html_text()
    asozdLink <- html_nodes(datePNode, xpath=".//a[contains(text(), 'Страница законопроекта в АСОЗД')]") %>% html_attr("href")
    if (length(asozdLink) == 0) {asozdLink = NA}
    
    scriptsText <- html_nodes(pg, xpath=".//script") %>% html_text()
    script <- scriptsText[11]
    startPosition <- regexpr("deputiesData = ", script)[1]
    endPosition <- regexpr("}];", script)[1]
    
    if (endPosition == -1) {
      vD <- c(vD, i)     
      next()
    }
    s <- substr(script, startPosition+15, endPosition+1)
    ld <- list("voteID" = voteID, "voteDate" = voteDate, "asozdLink" = asozdLink, "s" = s)
    nn <- nn + 1
    l[[nn]] <- ld
    if ((nn %% 5000) == 0) {
      saveRDS(l, paste0("l", nn,".rds")) 
    }   
  }
  saveRDS(l, paste0("l", nn,".rds"))  
}

readVotes2 <- function(){
  
  files <- list.files(,"^dt", full.names=T, recursive = TRUE)
  dd <- data.frame(stringsAsFactors = FALSE)
  for (i in 1:length(files)) {
    print(files[i])
    d <- readRDS(files[i])
    dd <- rbind(dd, d, stringsAsFactors = FALSE)
  }
  
  files <- list.files(,"^empty", full.names=T, recursive = TRUE)
  x <- c()
  for (i in 1:length(files)) {
    xx <- readRDS(files[i])
    x <- c(x, xx)
  }        
  
}

readVotes3 <- function(){
  
  df <- data.frame(convocation = character(),
                   depName= character(), 
                   depID = character(),
                   depFaction = character(), 
                   depfactionID = character(), 
                   depIsCurrent = logical(), 
                   depVoteResult = character(), 
                   voteDate = character(),
                   voteDateAPI = character(),
                   voteID = character(), 
                   voteSubject = character(), 
                   voteLawNumber = character(),
                   voteAsozdLink = character(),
                   voteFor = character(), 
                   voteAgainst = character(),
                   voteAbstain = character(),
                   voteAbsent = character(),
                   voteResultType = character(),
                   voteResult = logical(),
                   stringsAsFactors = FALSE)   
  
  x <- fread("mainDD.csv")
  xdt <- x[, c(-1,-2,-4)]
  dt <- as.tbl(xdt)
  dt <- dt %>% rename(depName = sortName, depFaction = faction, depFactionID = factionCode, depVoteResult = result, voteAsozdLink = asozdLink)
  dt$url <- gsub("/?convocation=", "", dt$url, fixed = TRUE)
  dt$convocation <- substr(dt$url, 1, 8)
  dt$depID <- substr(dt$url, 17, 24)
  dt <- dt %>% select(-url)
  df <- as.data.frame(dt)
  
  x <- fread("mainDDready.csv")
  dt <- as.tbl(x)
  #35015676
  x1 <- fread("voteList.csv")
  dt1 <- as.tbl(x1)
  #78601
  dt1 <- dt1 %>% rename(voteID = id, voteSubject = subject, voteForCount = forCount, voteAgainstCount = againstCount, voteAbstainCount = abstainCount, voteAbsentCount = absentCount, voteResultType = resultType, voteResult = result)
  dt1$voteID <- as.integer(dt1$voteID)
  dtt <- left_join(dt, dt1, by="voteID")
  dtt$voteDate.x <- dmy_hms(dtt$voteDate.x, tz="Europe/Moscow")
  dtt$voteDate.y <- ymd_hms(dtt$voteDate.y, tz="Europe/Moscow")
  dtt$depVoteResult[dtt$depVoteResult == 2] <- "absent"
  dtt$depVoteResult[dtt$depVoteResult == -1] <- "for"
  dtt$depVoteResult[dtt$depVoteResult == 1] <- "against"
  dtt$depVoteResult[dtt$depVoteResult == 0] <- "abstain"
  dtt$voteResult <- as.logical(dtt$voteResult)
  dtt$voteCount <- as.integer(dtt$voteCount)
  dtt$voteForCount <- as.integer(dtt$voteForCount)
  dtt$voteAgainstCount <- as.integer(dtt$voteAgainstCount)
  dtt$voteAbsentCount <- as.integer(dtt$voteAbsentCount)
  dtt$voteAbstainCount <- as.integer(dtt$voteAbstainCount)
  dtt$lawNumber <- substr(dtt$voteAsozdLink, 63, 70)
  dtt <- dtt %>% select(-V1, -convocation.y, -voteDate.y) 
  dtt <- dtt %>% rename(voteDate = voteDate.x, convocation = convocation.x)
  dtt <- left_join(dtt, depList, by="depID")
  dtt <- dtt %>% select(convocation, depID, depName, depIsCurrent, depFaction, depFactionID, depVoteResult, voteID, voteDate, voteAsozdLink, voteSubject, lawNumber, voteCount, voteForCount, voteAgainstCount, voteAbstainCount, voteAbsentCount, voteResultType, voteResult)
  df <- as.data.frame(dtt)     
  fwrite(df, "mainDDall.csv")
  df <- as.data.frame(dtt1)     
  fwrite(df, "mainDDall.csv")
  
  df <- fread("mainDDall.csv")
  x1 <- c("1-й созыв", "2-й созыв", "3-й созыв", "4-й созыв", "5-й созыв", "6-й созыв", "7-й созыв")
  x2 <- c("11.01.1994", "16.01.1996", "18.01.2000", "29.12.2003", "24.12.2007", "21.12.2011", "05.10.2016")
  x2 <- as.character(as.Date(x2, "%d.%m.%Y"))
  x3 <- c("AAAAAAA1", "AAAAAAA2", "AAAAAAA3", "AAAAAAA4", "AAAAAAA5", "AAAAAAA6", "AAAAAAA7")
  for (i in 1:length(x3)){
    df$convocationName[df$convocation == x3[i]] <- x1[i]
    df$convocationStartDate[df$convocation == x3[i]] <- x2[i]
  }
  dt <- dt %>% rename(depPerform = perform, depLaw = law, depBirthday = depBirthday.y, depStart = depStart.y) %>% select(convocation, convocationName, convocationStartDate, depID, depName, depIsCurrent, depBirthday, depStart, depPerform, depLaw, depFaction, depFactionID, depVoteResult, voteID, voteDate, voteAsozdLink, voteSubject, lawNumber, voteCount, voteForCount, voteAgainstCount, voteAbstainCount, voteAbsentCount, voteResultType, voteResult)
  
  dt <- left_join(dt, d1, by = c("depID" = "depID", "convocation" = "convocation"))    
  dt <- dt %>% select(- depBirthday.x, -depStart.x, -depPerform, -depLaw)
  dt1<- dt
  dt1$depBirthday <- as.Date(dt1$depBirthday, "%Y-%m-%d")
}


getDepList <- function() {
  query <- "http://api.duma.gov.ru/api/eeb67d69ed0a04188e7096b10acc7f928ca084e2/deputies.json?app_token=appf939b224250c65535cab8cfa9562377ea37a4d96"
  depListJSON <- GET(query)
  #document <- fromJSON(depListJSON, method='C')
  depList <- content(depListJSON)
  
  for(i in 1:length(depList)){
    depList[[i]]$factions <- ""
  }
  
  depList <- rbindlist(depList, fill=TRUE)
  depList$id <- as.integer(depList$id)
  depList <- as.tbl(depList)
  depList <- depList %>% rename(depID = id, depIsCurrent = isCurrent) 
  l <- list()
  for(i in 1:nrow(depList)){
    query <- paste0("http://api.duma.gov.ru/api/eeb67d69ed0a04188e7096b10acc7f928ca084e2/deputy.json?app_token=appf939b224250c65535cab8cfa9562377ea37a4d96&id=", depList$depID[i]) 
    depListJSON <- GET(query)
    dep <- content(depListJSON)
    l[[i]] <- dep
    print(i)
  } 
  for(i in 1:length(depList)){
    l[[i]]$educations <- ""
    l[[i]]$regions <- ""
    l[[i]]$degrees <- ""
    l[[i]]$ranks <- ""
    l[[i]]$activity <- ""
  }        
  ll <- rbindlist(l, fill=TRUE)
  #http://api.duma.gov.ru/api/:token/deputy.json?id=99100142   
  
  ddd <- unique(dc$depID)
  x <- c()
  
  for(i in 1:nrow(dc)){
    x <- c(x, paste0("http://www.duma.gov.ru/rewrite/?deputy=", dc$depID[i],"&sozyv=", dc$convocation[i]))
  }
  
  l <- list()
  files <- list.files("dep","*", full.names=T, recursive = TRUE)
  for (i in 1:length(files)) {
    voteFile <- files[i]
    startPosition <- regexpr("dep/index.html@deputy=", voteFile)[1]
    endPosition <- regexpr("&sozyv=", voteFile)[1]
    depID <- substr(voteFile, startPosition+22, endPosition-1)
    print(depID)
    convocation = substr(voteFile, endPosition+7, nchar(voteFile))
    print(convocation)
    
    pg <- read_html(voteFile, encoding = "UTF-8")
    datePNode <- html_nodes(pg, xpath=".//p[contains(text(), 'Дата начала полномочий')]") %>% html_text() 
    datePNode1 <- html_nodes(pg, xpath=".//p[contains(text(), 'Дата рождения:')]") %>% html_text()
    datePNode2 <- html_nodes(pg, xpath=".//li[@class='di-perfom']/a/span") %>% html_text() 
    datePNode3 <- html_nodes(pg, xpath=".//li[@class='di-law']/a/span") %>% html_text() 
    ll <- list(depID = depID, convocation = convocation, d1 = datePNode, d2 = datePNode1, perform = datePNode2, law = datePNode3, link = voteFile)
    print("------------")
    print(i)
    print(ll)
    l[[i]] <- ll
  }
  d <- data.frame(depID = character(), convocation = character(), depStart = character(), depBirthday = character(), d1 = character(), d2 = character(), perform = character(), law = character(), link = character(), stringsAsFactors = FALSE)
  for(i in 1:length(l)){
    ll <- l[[i]]
    d[i, ]$depID <- ll$depID
    d[i, ]$convocation <- ll$convocation
    d[i, ]$d1 <- if(length(ll$d1)==0) "" else ll$d1[1]
    d[i, ]$d2 <- if(length(ll$d2)==0) "" else ll$d2[1]
    d[i, ]$perform <- if(length(ll$perform)==0) "" else ll$perform[1]
    d[i, ]$law <- if(length(ll$law)==0) "" else ll$law[1]
    d[i, ]$link <- if(length(ll$link)==0) "" else ll$link[1]
  }
  
  d1 <- d
  x <- c(" января ", " февраля ", " марта ", " апреля ", " мая ", " июня ", " июля ", " августа ", " сентября ", " октября ", " ноября ", " декабря ")
  x1 <- c(".1.", ".2.", ".3.", ".4.", ".5.", ".6.", ".7.", ".8.", ".9.", ".10.", ".11.", ".12.")
  
  depStart <- c()
  depBirthday <- c()
  for (i in 1:nrow(d1)) {
    startPosition <- regexpr("Дата начала полномочий: ", d1[i, ]$d1)[1]
    endPosition <- regexpr(" года", d1[i, ]$d1)[1]
    date1 <- substr(d1[i, ]$d1, startPosition+24, endPosition-1)
    for (k in 1:length(x)) {
      date1 <- gsub(x[k], x1[k], date1)
    }
    d1[i, ]$depStart <- as.character(as.Date(date1, "%d.%m.%Y"))
    
    startPosition <- regexpr("Дата рождения: ", d1[i, ]$d2)[1]
    endPosition <- regexpr(" года", d1[i, ]$d2)[1]
    date2 <- substr(d1[i, ]$d2, startPosition+15, endPosition-1)
    for (k in 1:length(x)) {
      date2 <- gsub(x[k], x1[k], date2)
    }
    d1[i, ]$depBirthday <- as.character(as.Date(date2, "%d.%m.%Y"))
    d1[i, ]$perform <- gsub("\\(|\\)", "", d1[i, ]$perform)
    d1[i, ]$law <- gsub("\\(|\\)", "", d1[i, ]$law)
    
  }
  d1$perform <- as.integer(d1$perform)
  d1$law <- as.integer(d1$law)
  d1$perform[is.na(d1$perform)] <- 0
  d1$law[is.na(d1$law)] <- 0
  d1 <- d1 %>% select(depID, convocation, depBirthday, depStart, perform, law)
  d1$depID <- as.integer(d1$depID)
  
  startPosition <- regexpr("Дата начала полномочий: ", d$d1)
  endPosition <- regexpr("}];", script)[1]
  
  # l <- list()
  for(i in 1:length(x)){
    voteFile <- x[i]
    pg <- read_html(voteFile, encoding = "UTF-8")
    datePNode <- html_nodes(pg, xpath=".//p[contains(text(), 'Дата начала полномочий')]") %>% html_text() 
    datePNode1 <- html_nodes(pg, xpath=".//p[contains(text(), 'Дата рождения:')]") %>% html_text()
    datePNode2 <- html_nodes(pg, xpath=".//li[@class='di-perfom']/a/span") %>% html_text() 
    datePNode3 <- html_nodes(pg, xpath=".//li[@class='di-law']/a/span") %>% html_text() 
    ll <- list(depID = dc$depID[i], convocation = dc$convocation[i], d1 = datePNode, d2 = datePNode1, perform = datePNode2, law = datePNode3, link = voteFile)
    print("------------")
    print(i)
    print(ll)
    l[[i]] <- ll
  }
  
}

readDetailedResult <- function(nnn){
  
  # emptyIDs <- c()
  # dd <- data.frame()
  # listL <- readRDS("l_1.rds")
  # 
  # ##
  # l <- list()
  # nn <- 0
  # for (i in 1:length(listL)) {
  #         nn <- nn + 1
  #         l[[nn]] <- listL[[i]]
  #         if ((i %% 5000) == 0) {
  #                 saveRDS(l, paste0("l", i,".rds")) 
  #                 l <- list()
  #                 nn <- 0
  #                 print(i)
  #         }   
  # }
  # saveRDS(l, paste0("l", i,".rds")) 
  # l <- list()
  # nn <- 0
  # print(i)        
  # ##
  
  #rm(list=ls())
  
  emptyIDs <- c()
  
  dd <- data.frame()
  N <- nnn
  listL <- readRDS(paste0("l", N, ".rds"))
  
  for (i in 1:length(listL)) {
    l <- listL[[i]]  
    voteID <- l$voteID
    voteDate <- l$voteDate
    asozdLink <- l$asozdLink
    sj <- fromJSON(l$s)
    d <- rbindlist(sj, fill=TRUE)
    if (length(sj) == 0) { 
      emptyIDs <- c(emptyIDs, voteID)
      next
    }
    if (is.null(voteID)) {voteID <- ""}
    d$voteID <- voteID
    d$voteDate <- voteDate
    d$asozdLink <- asozdLink
    print(paste("voteN:", i, "voteID:", voteID, "df total:", nrow(dd)))  
    dd <- rbind(dd, d, stringsAsFactors = FALSE)
  }
  saveRDS(dd, paste0("dt", N, ".rds"))
  saveRDS(emptyIDs, paste0("emptyIDs", N, ".rds"))
}



