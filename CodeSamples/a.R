GetM <- function() {
    matrix(0, 9, 9)
}

PrepareDirectionList <- function() {
    mn <- matrix(0, 3, 3)
    mn <- as.data.frame(mn)
    mn[1, c(1, 2)] <- c(-1, 0)
    mn[2, c(1, 2)] <- c(0, 1)
    mn[3, c(1, 2)] <- c(1, 0)
    mn[, 3] <- c("w", "n", "e")
    
    mw <- matrix(0, 3, 3)
    mw <- as.data.frame(mw)
    mw[1, c(1, 2)] <- c(0, -1)
    mw[2, c(1, 2)] <- c(-1, 0)
    mw[3, c(1, 2)] <- c(0, 1)    
    mw[, 3] <- c("s", "w", "n")
    
    ms <- matrix(0, 3, 3)
    ms <- as.data.frame(ms)
    ms[1, c(1, 2)] <- c(1, 0)
    ms[2, c(1, 2)] <- c(0, -1)
    ms[3, c(1, 2)] <- c(-1, 0)    
    ms[, 3] <- c("e", "s", "w")
    
    me <- matrix(0, 3, 3)
    me <- as.data.frame(me)
    me[1, c(1, 2)] <- c(0, 1)
    me[2, c(1, 2)] <- c(1, 0)
    me[3, c(1, 2)] <- c(0, -1)   
    me[, 3] <- c("n", "w", "s")
    
    list(n = mn, w = mw, s = ms, e = me)
}

ShoWPersp <- function(matrixSize, leaveTail = FALSE) {
    
    m <- matrix(0, matrixSize, matrixSize)
    mt <- matrix(0, 4, 2)
    startXY <- c(round(matrixSize/2 + 1), round(matrixSize/2 + 1))
    mt[4, ] <- startXY
    Direction <- "n"
    directionList <- PrepareDirectionList()
    
    xy<- mt[4, ]

    for (i in 1:50) {

        newDirection <- round(runif(1, 1, 3))
        newAddXY <- as.numeric(directionList[[Direction]][newDirection, c(1,2)])
        Direction1 <- as.character(directionList[[Direction]][newDirection, c(3)])
        xy1 <- xy + newAddXY
        
        while (xy1[1] > matrixSize || xy1[2] > matrixSize || xy1[1] < 1 || xy1[2] < 1 ) {
            newDirection <- round(runif(1, 1, 3))
            newAddXY <- as.numeric(directionList[[Direction]][newDirection, c(1,2)])
            Direction1 <- as.character(directionList[[Direction]][newDirection, c(3)])
            xy1 <- xy + newAddXY            
        }
        
        Direction <- Direction1
        xy <- xy1

        mt <- AddToMatrix(mt, xy)
        x <- mt[1, 1]
        y <- mt[1, 2]
        m[x, y] <- 0.3

        for (k in 2:4) {
            x <- mt[k, 1]
            y <- mt[k, 2]
            m[x, y] <- 1            
        }
        persp(m, expand = 0.3)
        Sys.sleep(0.5)
        
    }
}

AddToMatrix <- function(m, newXY) {
    for (i in 2:4) {
        m[i-1, ] <- m[i, ]
    }
    m[4, ] <- newXY
    m
}