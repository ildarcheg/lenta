
envir <-function(y) {
    op <- function(y) {
        x <- x + y
        print(x) 
    }
    op(y)
    print(x) 
}

if1 <- function(x) {
    y <- if (x>3) {"more than three"}
    else if (x>=0 && x <=3) {"from 0 to 3"}
    else {"less then 0"}
    print(y)
}





return_func <- function(x) {
    op <- function() {
        x <- x+ 5
    }
}

for1 <- function(x) {
    for(i in 1:x) {
        print(class(i[1]))
        print(class(i[[1]]))
    }
}

for2 <- function(x = c("a", "b", "c", "d")) {
    for(i in 1:4){
        print(x[i])
    }
    
    for(i in seq_along(x)) {
        print(x[i])
    }
    
    for(letter in x){
        print(letter)
    }
    
    for(i in 1:4) print(x[i])
}

for3 <- function(x = matrix(1:6, 2, 3)){
    x
}

while1 <- function(x){
    count <-0
    while(count < x) {
        print(count)
        count <- count +1
    }
}

while2 <- function(z) {
    while(z>=3 && z <=10) {
        print(z)
        coin <- rbinom(1, 1, 0.5)
        
        if(coin == 1) {
            z <- z + 1
        } else {
            z <- z - 1
        }
    }
}


make.power <- function(n) {
    pow <- function(x) {
        x^n
    }
    pow
}

g <- function(x) {
    a <-3
    x+a+y
}


make.NegLogLik <- function(data, fixed=c(FALSE,FALSE)) {
    params <- fixed
    function(p) {
        params[!fixed] <- p
        mu <- params[1]
        sigma <- params[2]
        a <- -0.5*length(data)*log(2*pi*sigma^2)
        b <- -0.5*sum((data-mu)^2) / (sigma^2)-(a + b)
    } 
}

getdate <- function(){
    datastring <- c("January 10, 2012 10:40", "December 9, 2011 9:10")
    x <- strptime(datastring, "%B %d, %Y %H:%M")
    x
}

makeVector <- function(x = numeric()) {
    m <- NULL
    set <- function(y) {
        x <<- y
        m <<- NULL
    }
    get <- function() x
    setmean <- function(mean) m <<- mean
    getmean <- function() m
    list(set = set, get = get,
         setmean = setmean,
         getmean = getmean)
}

cachemean <- function(x, ...) {
    m <- x$getmean()
    if(!is.null(m)) {
        message("getting cached data")
        return(m)
    }
    data <- x$get()
    m <- mean(data, ...)
    x$setmean(m)
    m
}

makeCacheMatrix <- function(incomingmatrix) {
    inversedmatrix <- NULL
    set <- function(y) {
        incomingmatrix <<- y
        inversedmatrix <<- NULL
    }
    get <- function() incomingmatrix
    setinversedmatrix <- function(newinversedmatrix) inversedmatrix <<- newinversedmatrix
    getinversedmatrix <- function() inversedmatrix
    list(set = set, get = get, setinversedmatrix = setinversedmatrix, getinversedmatrix = getinversedmatrix)
}

cacheSolve <- function(x, ...) {
    inversedmatrix <- x$getinversedmatrix()
    if(!is.null(inversedmatrix)) {
        message("getting cached data (inversed matrix)")
        return(inversedmatrix)        
    }
    incomingmatrix <- x$get()
    inversedmatrix <- solve(incomingmatrix)
    x$setinversedmatrix(inversedmatrix)
    inversedmatrix
}