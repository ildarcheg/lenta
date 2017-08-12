splitInChunks <- function(x, chunk.seq, chunk.delimiter, keep.delimiter) {
  
  chunk.list <- list()
  for (i in 1:(length(chunk.seq)-1)) {
    first.chunk.position <- chunk.seq[i]
    last.chunk.position <- chunk.seq[i+1] - 1
    chunk.vector <- x[first.chunk.position:last.chunk.position]
    if (!is.null(chunk.delimiter)&!keep.delimiter){
      chunk.vector <- chunk.vector[chunk.vector!=chunk.delimiter]
    }
    chunk.list[[i]] <- chunk.vector
  }  
  
  names(chunk.list) <- 1:(length(chunk.seq) - 1)
  
  return(chunk.list) 
}

chunk <- function(x, n.chunks = NULL, chunk.size = NULL, chunk.delimiter = NULL, 
                  fixed.delimiter = FALSE, keep.delimiter = FALSE) {
  
  x.length <- length(x)
  
  if (!is.null(chunk.size)) {
    chunk.seq <- seq(from = 1, to = x.length, by = chunk.size)
    chunk.seq <- c(chunk.seq, x.length + 1)
    chunk.list <- splitInChunks(x, chunk.seq, NULL, TRUE)
    return(chunk.list)
  }
  
  if (!is.null(n.chunks)) {
    chunk.size <- ceiling(x.length/n.chunks)
    chunk.seq <- seq(from = 1, to = n.chunks*chunk.size, by = chunk.size)
    chunk.seq <- c(chunk.seq, n.chunks*chunk.size + 1)
    chunk.list <- splitInChunks(x, chunk.seq, NULL, TRUE)
    return(chunk.list)
  }  
  
  if (!is.null(chunk.delimiter)) {
    lengthOfX <- length(x)
    chunk.seq <- c(1, grep(x, pattern = chunk.delimiter, fixed = fixed.delimiter), 
                   lengthOfX + 1)
    chunk.list <- splitInChunks(x, chunk.seq, chunk.delimiter, keep.delimiter)
    return(chunk.list)
  } 
  
}
