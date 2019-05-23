library("readr")
library("dplyr")

library("tidyr")

download.date = "05_20_2019"
filepath = sprintf("../DataBySensor/Sentry/download/%s/", download.date)
files = list.files(path=filepath,
                   pattern = "*.csv")

options(tibble.width=Inf)

## following summarizes the min and max for each variable, each row is a file x month

acc = NULL
for (f in files) {
  print(f)
  df <- readr::read_csv(paste0(filepath, f)) %>%
    tibble::as.tibble() %>%
    dplyr::mutate(posix.timestamp = as.POSIXct(timestamp, format="%m/%d/%Y %H:%M")) %>%
    dplyr::select(-timestamp) %>%
    {.}
  df1 = df %>%
    dplyr::mutate(month=format(posix.timestamp, "%m")) %>%
    dplyr::select(-posix.timestamp) %>%
    dplyr::group_by(month) %>%
    dplyr::summarise_all(list("min"=min, "max"=max)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(filename=f) %>%
    ## tidyr::gather(variable, value, `temp_min`:`water_max`) %>%
    {.}
  df1 <- df1 %>%
    dplyr::mutate(start = min(df$posix.timestamp), end = max(df$posix.timestamp))
  acc <- rbind(acc, df1)
}

acc %>%
  readr::write_csv(sprintf("../DataBySensor/Sentry/stat_summary_%s_each_month_a_row.csv", download.date))

## following summarizes the min and max for each variable, each row is a file
f = files[1]

acc = NULL
for (f in files) {
  print(f)
  df <- readr::read_csv(paste0(filepath, f),
                        col_types = cols(water = col_double())) %>%
    tibble::as.tibble() %>%
    dplyr::mutate(posix.timestamp = as.POSIXct(timestamp, format="%m/%d/%Y %H:%M")) %>%
    dplyr::select(-timestamp) %>%
    {.}
  df1 = df %>%
    dplyr::mutate(month=format(posix.timestamp, "%m")) %>%
    dplyr::select(-posix.timestamp) %>%
    dplyr::group_by(month) %>%
    dplyr::summarise_all(list("min"=min, "max"=max)) %>%
    dplyr::ungroup() %>%
    tidyr::gather(variable, value, `temp_min`:`water_max`) %>%
    tidyr::unite(var, c("variable", "month"), sep = " ") %>%
    dplyr::mutate(filename = f) %>%
    ## tidyr::spread(var, value) %>%
    {.}
  acc <- rbind(acc, df1)
}

acc %>%
  tidyr::spread(var, value) %>%
  readr::write_csv(sprintf("../DataBySensor/Sentry/stat_summary_%s_each_file_a_row.csv", download.date))

head(acc)

df1 <- df1 %>%
  dplyr::mutate(start = min(df$posix.timestamp), end = max(df$posix.timestamp))
acc <- rbind(acc, df1)

result <- do.call(cbind, lapply(df, summary)) %>%
  tibble::as.tibble() %>%
  dplyr::mutate(filename=f) %>%
  dplyr::mutate(stats = c("Min.", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max.")) %>%
  tidyr::gather(variable, value, temp:posix.timestamp, factor_key = TRUE) %>%
  dplyr::filter((variable != "posix.timestamp") | (stats %in% c("Min.", "Max."))) %>%
  tidyr::unite(var, c("stats", "variable"), sep = " ") %>%
  tidyr::spread(var, value) %>%
  dplyr::rename(start=`Min. posix.timestamp`, end=`Max. posix.timestamp`) %>%
  dplyr::select(filename, start, end,
                `Min. rh`, `1st Qu. rh`, `Median rh`, `Mean rh`, `3rd Qu. rh`, `Max. rh`,
                `Min. dp`, `1st Qu. dp`, `Median dp`, `Mean dp`, `3rd Qu. dp`, `Max. dp`,
                `Min. temp`, `1st Qu. temp`, `Median temp`, `Mean temp`, `3rd Qu. temp`, `Max. temp`,
                `Min. water`, `1st Qu. water`, `Median water`, `Mean water`, `3rd Qu. water`, `Max. water`,
                ) %>%
  dplyr::mutate_at(vars(`start`, `end`), as.POSIXct, origin = '1970-01-01') %>%
  {.}
acc <- rbind(acc, result)


acc = NULL
for (f in files) {
  print(f)
  df <- readr::read_csv(paste0(filepath, f)) %>%
    tibble::as.tibble() %>%
    dplyr::mutate(posix.timestamp = as.POSIXct(timestamp, format="%m/%d/%Y %H:%M")) %>%
    dplyr::select(-timestamp) %>%
    {.}
  result <- do.call(cbind, lapply(df, summary)) %>%
    tibble::as.tibble() %>%
    dplyr::mutate(filename=f) %>%
    dplyr::mutate(stats = c("Min.", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max.")) %>%
    tidyr::gather(variable, value, temp:posix.timestamp, factor_key = TRUE) %>%
    dplyr::filter((variable != "posix.timestamp") | (stats %in% c("Min.", "Max."))) %>%
    tidyr::unite(var, c("stats", "variable"), sep = " ") %>%
    tidyr::spread(var, value) %>%
    dplyr::rename(start=`Min. posix.timestamp`, end=`Max. posix.timestamp`) %>%
    dplyr::select(filename, start, end,
                  `Min. rh`, `1st Qu. rh`, `Median rh`, `Mean rh`, `3rd Qu. rh`, `Max. rh`,
                  `Min. dp`, `1st Qu. dp`, `Median dp`, `Mean dp`, `3rd Qu. dp`, `Max. dp`,
                  `Min. temp`, `1st Qu. temp`, `Median temp`, `Mean temp`, `3rd Qu. temp`, `Max. temp`,
                  `Min. water`, `1st Qu. water`, `Median water`, `Mean water`, `3rd Qu. water`, `Max. water`,
                  ) %>%
    dplyr::mutate_at(vars(`start`, `end`), as.POSIXct, origin = '1970-01-01') %>%
    {.}
  acc <- rbind(acc, result)
}

acc %>%
  readr::write_csv(sprintf("../DataBySensor/Sentry/stat_summary_%s.csv", download.date))
