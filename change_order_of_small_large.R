library(readr)
library(dplyr)

setwd("~/Dropbox/ROCIS/DataBySensor/Dylos/to_calibrate/022618/")

filelist = list.files()
filelist <- filelist[filelist != "clean"]
filelist <- filelist[filelist != "plot"]

## for (i in 1:length(filelist)) {
## for (i in 15:(length(filelist) - 1)) {
for (i in 1:1) {
  print(i)
  print(filename)
  filename = filelist[i]
  df = readr::read_delim(filename, delim="\t")
  print(names(df))
  colnames(df) <- c("Date/Time","Small","Large")
  sapply(df, class)
  df <- df[-(1:3), ]
  ## df %>% readr::write_csv(paste0("clean/", filename, ".txt"))
  print(head(df))
  df %>% readr::write_delim(paste0("clean/", filename, ".txt"), delim=",")
}
