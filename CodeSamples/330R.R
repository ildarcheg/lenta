library(stringr)
library(dplyr)
library(lubridate)
library(gridExtra)
library(grid)
library(gtable)
library(ggplot2)
library(purrr)
library(wordcloud)
library(tidyr)

mainFolder <- "~/330"
tempFolder <- "temp"
downloadFolder <- "download"

BaseURL <- "forum330.com/forum/member/activity/339/"

if (!file.exists(file.path(mainFolder))) {
        dir.create(file.path(mainFolder))
}
setwd(file.path(mainFolder))
if (!file.exists(file.path(mainFolder, tempFolder))) {
        dir.create(file.path(mainFolder, tempFolder))
}
if (!file.exists(file.path(mainFolder, downloadFolder))) {
        dir.create(file.path(mainFolder, downloadFolder))
}


ProcessData <- function() {
        
        df <- readRDS("df_3.rds")
        dt <- as.tbl(df) 
        dt <- dt[!is.na(dt$name), ] %>% arrange(topicID, messageID)        

        dt <- dt %>% mutate(topicMessageId = paste0(topicID, "-", messageID), replyToTopicMessageID = paste0(replyToTopicID, "-", replyToMessageID))
        dtR <- select(dt, name, replyToTopicMessageID = topicMessageId)
        dt <- left_join(dt, dtR, by = "replyToTopicMessageID") 
        dt <- rename(dt, name = name.x, nameTo = name.y)
        
        setAction <- function(elem) {
                if (grepl("начал обсуждение", elem))  {
                        return("NewTopic")
                } else {
                        return("NewMessage")
                }
        }
        
        dt$action <- sapply(dt$action, setAction)

        mess <- dt$message
        stop_symbols <-
                c(".",
                  ",",
                  "!",
                  "?",
                  ":",
                  ";",
                  "-",
                  "+",
                  "—",
                  "\n",
                  "(",
                  ")",
                  "«",
                  "»",
                  "'",
                  '"')
        for (i in 1:length(stop_symbols)) {
                mess <- gsub(stop_symbols[i], " ", mess, fixed = TRUE)
                mess <- gsub("   ", " ", mess, fixed = TRUE)
                mess <- gsub("  ", " ", mess, fixed = TRUE)
                mess <- str_trim(mess)
        }
        
        dt <- dt %>% mutate(message2 = "")
        dt$message2 <- mess
        
        saveRDS(dt, "data_tbl.rds")
}

Lemma <- function() {
        dt <- readRDS("data_tbl.rds")
        df <- as.data.frame(dt)
        
        n <- nrow(df)
        for (i in 1:ceiling(n/100000)) {
                begin <- (i-1)*100000 + 1
                end <- min(i*100000, n)
                print(paste0(begin, " ", end))
                write.csv2(dt$message2[begin:end], paste0("lemma", i, "csv"), fileEncoding = "UTF-8", row.names = FALSE)
        } 
        
        l <- list()
        l[[1]] <- read.csv2("lemmas.csv", stringsAsFactors = FALSE)
        l[[2]] <- read.csv2("lemmas-2.csv", stringsAsFactors = FALSE)
        l[[3]] <- read.csv2("lemmas-3.csv", stringsAsFactors = FALSE)
        l[[4]] <- read.csv2("lemmas-4.csv", stringsAsFactors = FALSE)
        l[[5]] <- read.csv2("lemmas-5.csv", stringsAsFactors = FALSE)
        l[[6]] <- read.csv2("lemmas-6.csv", stringsAsFactors = FALSE)
        df1 <- bind_rows(l[[1]][1:100000, ], l[[2]][1:100000, ], l[[3]][1:100000, ], l[[4]][1:100000, ],l[[5]][1:100000, ], l[[6]][1:49010, ])
        dt
}
PrepareDFforLinks <- function() {
        
}
GetTableCode <- function(tableList, ncol, nrow) {
        tableCode <- c("<table>")
        for (i in 1:nrow) {
                tableCode <- c(tableCode, "<tr>")
                for (k in 1:ncol) {
                        
                        tableNum <- (i-1)*k + k
                        print(tableNum)
                        tableName <- names(tableList)[tableNum]
                        print(tableName)
                        if (is.na(tableName)) {tableName <- ""}
                        tableCode <- c(tableCode, "<td><b><center>", tableName, "</center></b></td>")
                        tableCode <- c(tableCode, "<td width=20>", " ", "</td>")
                        
                }
                tableCode <- c(tableCode, "</tr>")
                tableCode <- c(tableCode, "<tr>")
                for (k in 1:ncol) {
                        
                        tableNum <- (i-1)*k + k
                        if (tableNum <= length(tableList)) {
                                tableForInsert <- tableList[[tableNum]]
                        } else {
                                tableForInsert <- ""        
                        }
                        tableCode <- c(tableCode, "<td>", tableForInsert, "</td>")
                        tableCode <- c(tableCode, "<td width=20>", " ", "</td>")
                }    
                tableCode <- c(tableCode, "</tr>")
                tableCode <- c(tableCode, "<tr height=20>")
                tableCode <- c(tableCode, "</tr>")
        }   
        tableCode <- c(tableCode, "</table>")
        return(tableCode)
}

GetStat <- function() {
        require(knitr)
        dt <- readRDS("data_tbl_lemma.rds")
        stop <- readRDS("stop.rds")
        lastDate <- max(dt$time)
        tableList <- list()
        df <- dt %>% count(name) %>% arrange(desc(n)) %>% rename('User name' = name, 'Messages' = n) %>% head(20) %>% as.data.frame()
        table <- kable(df, format='html', output = FALSE)
        tableList["Top (total)"] <- table
        yearAgo <- lastDate - years(1)
        df <- dt %>% filter(time >= yearAgo) %>% count(name) %>% arrange(desc(n)) %>% rename('User name' = name, 'Messages' = n) %>% head(20) %>% as.data.frame()
        table <- kable(df, format='html', output = FALSE)
        tableList["Top (last year)"] <- table
        
        tableCode <- GetTableCode(tableList, 2, 1)
        cat(tableCode, sep = '')
        
        
        tableList <- list()
        for (i in 1:12) {
                monthAgo <- lastDate - months(i-1)
                end <- ceiling_date(monthAgo, "month")
                begin <- floor_date(monthAgo, "month")
                df <- dt %>% filter((time >= begin) & (time < end)) %>% count(name) %>% arrange(desc(n)) %>% rename('User name' = name, 'Messages' = n) %>% head(10) %>% as.data.frame()
                table <- kable(df, format='html', output = FALSE)
                tableName <- format(begin, format="%B %Y")
                tableList[tableName] <- table
        }
        
        tableCode <- GetTableCode(tableList, 4, 3)
        cat(tableCode, sep = '')  
        
        dtDayActivity <- dt %>% 
                mutate(hour = hour(round_date(time, "hour")), day = floor_date(time, "day"), month = as.factor(floor_date(time, "month"))) %>% 
                filter(action == "NewMessage") %>% 
                select(name, day, hour, month, time) 
        
        dtDayActivityByMonth <- dtDayActivity %>%
                filter(time > ceiling_date(yearAgo, "month")) %>%
                count(hour, day, month) %>% 
                group_by(hour, month) %>% 
                summarise(nMean = mean(n), nMedian = median(n), n = mean(n))
        
        dtDayActivityTotal <- dtDayActivity %>%
                count(hour, day) %>% 
                group_by(hour) %>% 
                summarise(nMean = mean(n), nMedian = median(n), n = mean(n))
        
        ggplot() + geom_line(data = dtDayActivityTotal, aes(x=hour, y=n)) + 
                theme(axis.text.x = element_text(size=5)) +
                scale_x_continuous(name ="Hours", breaks=seq(0,23,1)) +
                scale_y_continuous(name ="Messages", limits = c(0,60), breaks=seq(0,60,10)) + 
                ggtitle("Hour's activity (total)")
        
        ggplot() + geom_line(data = dtDayActivityByMonth, aes(x=hour, y=n)) + 
                facet_wrap( ~ month, scales = "free", ncol = 2) + 
                theme(axis.text.x = element_text(size=5)) +
                scale_x_continuous(name ="Hours", breaks=seq(0,23,1)) +
                scale_y_continuous(name ="Messages", limits = c(0,60), breaks=seq(0,60,10)) + 
                ggtitle("Hour's activity (by month)")
        
        
        dtUsers <- dt %>% filter(time > ceiling_date(yearAgo, "month")) %>% 
                count(name) %>% 
                arrange(desc(n)) %>% 
                rename('User name' = name, 'Messages' = n) %>% 
                head(30)
        userList <- dtUsers$`User name`
        #userFilter <- c("Старый чайник", "Guk", "Эльниньо", "sf")
        userFilter <- userList
        dtDayActivity <- dt %>% 
                filter(name %in% (userFilter)) %>%
                mutate(hour = hour(round_date(time, "hour")), day = floor_date(time, "day"), name = as.factor(name)) %>% 
                filter(action == "NewMessage") %>% 
                select(name, day, hour, name, time) 
        dtDayActivityByName <- dtDayActivity %>%
                filter(time > ceiling_date(yearAgo, "month")) %>%
                count(hour, day, name) %>% 
                group_by(hour, name) %>% 
                summarise(nMean = mean(n), nMedian = median(n), n = mean(n))
        ggplot() + geom_line(data = dtDayActivityByName, aes(x=hour, y=n)) + 
                facet_wrap( ~ name, scales = "free", ncol = 2) + 
                theme(axis.text.x = element_text(size=5)) +
                scale_x_continuous(name ="Hours", breaks=seq(0,23,1)) +
                scale_y_continuous(name ="Messages", limits = c(0,5), breaks=seq(0,5,1)) + 
                ggtitle("Hour's activity (by name)")

        hh <- function(h1,h2) {
                return(paste(h1, h2, collapse = "|"))
        }
        
        dtLinks <- dt %>% filter(!is.na(links)) %>% mutate(id = "") %>% select(name, links, id)
        dtLinks$links <- gsub(pattern = "|", replacement = ":::::::", x = dtLinks$links, fixed = TRUE) 
        dfl <- dtLinks %>% separate_rows(links, sep = ":::::::")
        link <- dfl$links
        link <- gsub(pattern = "http://", replacement = "", x = link, fixed = TRUE)
        link <- gsub(pattern = "https://", replacement = "", x = link, fixed = TRUE)
        link <- gsub(pattern = '"', replacement = "", x = link, fixed = TRUE)
        link <- gsub(pattern = " ", replacement = "", x = link, fixed = TRUE)
        
        clearLink <- function(linkOne) {
                pattern <- "/"
                linkOne <- str_split(linkOne, fixed(pattern))[[1]][1]  
                pattern <- "."
                linkOne <- str_split(linkOne, fixed(pattern))[[1]]
                linkOne <- paste0(linkOne[length(linkOne)-1],".",linkOne[length(linkOne)])
        }
        dfl$links <- sapply(link, clearLink)
        
        # for (i in 1:nrow(dtLinks)) {
        #         dtLinks$id[i] <- paste(c(dtLinks$name[i], dtLinks$links[i]), sep ="", collapse = "|")
        # }
        # linksList <- dtLinks$id
        # 
        # GetLink <- function(infoLink) {
        # 
        #         pattern = "|"
        #         x <- str_split(infoLink, fixed(pattern))[[1]]
        #         nameC <- x[1]
        #         l <- list()
        #         for (i in 2:length(x)) {
        #                 link <- x[i]
        #                 link <- gsub(pattern = "http://", replacement = "", x = link, fixed = TRUE)
        #                 link <- gsub(pattern = "https://", replacement = "", x = link, fixed = TRUE) 
        #                 link <- gsub(pattern = " ", replacement = "", x = link, fixed = TRUE) 
        #                 pattern <- "/"
        #                 link <- str_split(link, fixed(pattern))[[1]][1]
        #                 pattern <- "."
        #                 link <- str_split(link, fixed(pattern))[[1]]
        #                 link <- paste0(link[length(link)-1],".",link[length(link)]) 
        #                 dfl <- data.frame(name = nameC, 
        #                                  link = link, 
        #                                  stringsAsFactors=FALSE) 
        #                 l[[i]] <- dfl
        #         }
        #         dfl <- do.call("rbind", l)
        # }
        # 
        # dfl <- map_df(linksList, GetLink)
        
        dtl1 <- dfl %>% as.tbl() %>% count(link) %>% arrange(desc(n)) %>% head(30) %>% as.data.frame()
        
        tableList <- list()
        dtl2 <- dfl %>% as.tbl() %>% filter(name %in% userFilter) 
        userLinkList <- unique(dtl2$name)
        for (i in 1:length(userLinkList)) {
                name1 <- userLinkList[i]
                print(name1)
                dtl2a <- dtl2 %>% filter(name == name1) %>% count(link) %>% arrange(desc(n)) %>% head(10) %>% as.data.frame()
                print(dtl2a)
                tableList[[name1]] <- dtl2a
        }
        tableCode <- GetTableCode(tableList, 5, 6)
        cat(tableCode, sep = '')   
        
        
        tableList <- list()
        dtTo <- dt %>% select(name, nameTo) %>% filter(!is.na(nameTo)) %>% count(name, nameTo)
        users <- dtTo %>% filter(name %in% userFilter) 
        users <- unique(users)
        for (i in 1:length(users)) {
                name1 <- users[i]
                dtl2a <- dtl2 %>% filter(name == name1) %>% arrange(desc(n)) %>% head(10) %>% as.data.frame()
                tableList[[name1]] <- dtl2a
        }
        tableCode <- GetTableCode(tableList, 5, 6)
        cat(tableCode, sep = '')  
        
       
        
         require(tm)
        require(wordcloud)
        require(RColorBrewer)
        u = "http://cran.r-project.org/web/packages/available_packages_by_date.html"
        t = readHTMLTable(u)[[1]]
        ap.corpus <- Corpus(DataframeSource(data.frame(as.character(t[,3]))))
        ap.corpus <- tm_map(ap.corpus, removePunctuation)
        ap.corpus <- tm_map(ap.corpus, tolower)
        ap.corpus <- tm_map(ap.corpus, function(x) removeWords(x, stopwords("english")))
        ap.tdm <- TermDocumentMatrix(ap.corpus)
        ap.m <- as.matrix(ap.tdm)
        ap.v <- sort(rowSums(ap.m),decreasing=TRUE)
        ap.d <- data.frame(word = names(ap.v),freq=ap.v)
        table(ap.d$freq)
        pal2 <- brewer.pal(8,"Dark2")
        png("wordcloud_packages.png", width=1280,height=800)
        wordcloud(ap.d$word,ap.d$freq, scale=c(8,.2),min.freq=3,
                  max.words=Inf, random.order=FALSE, rot.per=.15, colors=pal2)
        
        
        dtH <- dt %>% separate_rows(lemmas, sep = " ") %>% 
                filter(!(lemmas %in% stop)) %>% filter(name == "Guk") %>%
                count(lemmas) %>% 
                arrange(desc(n))
        #png("wordcloud_packages.png")
        pal2 <- brewer.pal(8,"Dark2")
        n <- 100
        wordcloud(dtH$lemmas[1:n], dtH$n[1:n]*c(n:1), scale=c(3,.05), min.freq=3, random.order=FALSE, rot.per=0.15, colors=pal2)


        dtH <- dt %>% separate_rows(lemmas, sep = " ") %>%
                filter(time > ceiling_date(yearAgo, "month")) %>%
                filter(!(lemmas %in% stop))
        users <- dtH %>% filter(name %in% userFilter)
        users <- unique(users$name)
        for (i in 1:length(users)) {
                name1 <- users[i]
                dtl2a <- dtH %>% filter(name == name1)  %>% select(lemmas) %>% count(lemmas) %>% arrange(desc(n))
                n <- 100
                print(name1)
                print(paste(dtl2a$lemmas[1:50], collapse = " "))
        }    
        #qplot(hour, n, data = dtDayActivityTotal, geom = c("line"))
        
        #ggplot() + geom_line(data = dtDayActivityByMonth, aes(x=hour, y=n, color = month)) + geom_line(data = dtDayActivityTotal, aes(x=hour, y=n, size = 1))
        #plot(c(0, 23), c(0, max(dtDayActivityTotal$n)), type = "n", xaxt = "n")
        #axis(1, xaxp=c(0, 24, 24), las=2)
        #lines(dtDayActivityTotal$hour, dtDayActivityTotal$n, lwd=1)   

}

temp <- function(n){
        pattern <- "/"
        n <- str_split(n, fixed(pattern))[[1]][1]  
        pattern <- "."
        n <- str_split(n, fixed(pattern))[[1]]
        n1 <- paste0(n[length(n)-1],".",n[length(n)]) 
        return(n1)
}
clearLink <- function(linkOne) {
        linkOne <- paste0(linkOne,"2")
        linkOne <- paste0(linkOne,"2")
}

clearLink2 <- function(linkOne) {
        pattern <- "/"
        n <- str_split(n, fixed(pattern))[[1]][1]  
        pattern <- "."
        n <- str_split(n, fixed(pattern))[[1]]
        n1 <- paste0(n[length(n)-1],".",n[length(n)])
        n1
}
Test <- function() {
        library(gridExtra)
        d <- head(iris)
        table <- tableGrob(d)
        
        library(grid)
        library(gtable)
        
        title <- textGrob("Title",gp=gpar(fontsize=50))
        footnote <- textGrob("footnote", x=0, hjust=0,
                             gp=gpar( fontface="italic"))
        
        padding <- unit(0.5,"line")
        table <- gtable_add_rows(table, 
                                 heights = grobHeight(title) + padding,
                                 pos = 0)
        table <- gtable_add_rows(table, 
                                 heights = grobHeight(footnote)+ padding)
        table <- gtable_add_grob(table, list(title, footnote),
                                 t=c(1, nrow(table)), l=c(1,2), 
                                 r=ncol(table))
        grid.newpage()
        grid.draw(table)
}

Test2 <- function() {
        library(grid)
        library(gridExtra)
        library(gtable)
        
        t1 <- tableGrob(head(iris))
        title <- textGrob("Title",gp=gpar(fontsize=50))
        padding <- unit(5,"mm")
        
        table <- gtable_add_rows(t1, 
                                 heights = grobHeight(title) + padding,
                                 pos = 0)
        table <- gtable_add_grob(table, title, 1, 1, 1, ncol(table))
        grid.arrange(table, table, nrow = 1)
        grid.newpage()
        grid.draw(table)
}

Test3 <- function() {
        library(gridExtra)
        justify <- function(x, hjust="center", vjust="center", draw=TRUE){
                w <- sum(x$widths)
                h <- sum(x$heights)
                xj <- switch(hjust,
                             center = 0.5,
                             left = 0.5*w,
                             right=unit(1,"npc") - 0.5*w)
                yj <- switch(vjust,
                             center = 0.5,
                             bottom = 0.5*h,
                             top=unit(1,"npc") - 0.5*h)
                x$vp <- viewport(x=xj, y=yj)
                if(draw) grid.draw(x)
                return(x)
        }
        
        g <- tableGrob(iris[1:3,1:2])
        grid.newpage()
        justify(g,"left", "top")
}

GetTables <- function(tableList, ncol, nrow) {
        tableCode <- c("<table>")
        for (i in 1:nrow) {
                tableCode <- c(tableCode, "<tr>")
                for (k in 1:ncol) {
                        
                        tableNum <- i*k
                        tableName <- names(tableList)[tableNum]
                        if (is.na(tableName)) {tableName <- ""}
                        tableCode <- c(tableCode, "<td>", tableName, "</td>")
    
                }
                tableCode <- c(tableCode, "</tr>")
                tableCode <- c(tableCode, "<tr>")
                for (k in 1:ncol) {
                        
                        tableNum <- i*k
                        if (tableNum <= length(tableList)) {
                                tableForInsert <- tableList[[tableNum]]
                        } else {
                                tableForInsert <- ""        
                        }
                        tableCode <- c(tableCode, "<td>", tableForInsert, "</td>")
                }    
                tableCode <- c(tableCode, "</tr>")
        }   
        c(tableCode, "</table>")
}


