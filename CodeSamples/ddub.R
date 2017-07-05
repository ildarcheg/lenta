ReadMainCSV <- function() {
    library(dplyr)
    library(lubridate)
    df <- read.csv("dubizzle.csv", stringsAsFactors=FALSE)
    dt <- tbl_df(df)
    nn <- gsub("value.", "", names(dt))
    names(dt) <- nn
    dt$Data <- as.Date(ymd(dt$Data))
    dt
}

# soul <- transform(xx, Year = factor(Year))
# > boxplot(Price ~ Year, soul, xlab = "Year", ylab = "Price")
# > xx <- filter(x, Model == "Soul" & (Year == "2013" | Year ==  "2014" | Year == 2015))
# > soul <- transform(xx, Year = factor(Year))
# > boxplot(Price ~ Year, soul, xlab = "Year", ylab = "Price")
# > xx <- filter(x, Model == "Elantra" & (Year == "2013" | Year ==  "2014" | Year == 2015))
# > elantra <- transform(xx, Year = factor(Year))
# > boxplot(Price ~ Year, elantra, xlab = "Year", ylab = "Price")
# > xx <- filter(x, Model == "Sportage" & (Year == "2013" | Year ==  "2014" | Year == 2015))
# > sportage <- transform(xx, Year = factor(Year))
# > boxplot(Price ~ Year, sportage, xlab = "Year", ylab = "Price")
