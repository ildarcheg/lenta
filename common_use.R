require(gdata)

TimeStamp <- function(prefix = "##------ ", suffix = " ------##", memory = TRUE) {
  timestamp(prefix = prefix, suffix = suffix)
  if (memory == TRUE) {
    ll(unit = "MB")
  }
}