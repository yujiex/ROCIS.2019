library(dplyr)
library(readxl)
library(readr)

setwd("~/Dropbox/ROCIS/routines")
getwd()

devtools::load_all("~/Dropbox/thesis/code/summaryUtil")

start_date = "03_24_2018"
prev_date = "05_12_2019"
now_date = "05_19_2019"

## only download one week of data
files = list.files(path=sprintf("~/Dropbox/ROCIS/DataBySensor/PurpleAir/download/%s/", now_date),
                   pattern = ".csv")

head(files)

length(files)

## combine new download and old files into one file
for (f in (files)) {
  print(f)
  file_sofar = gsub(sprintf("%s %s", prev_date, now_date), sprintf("%s %s", start_date, prev_date), f)
  print(file_sofar)
  df_sofar = readr::read_csv(sprintf("~/Dropbox/ROCIS/DataBySensor/PurpleAir/sofar/%s", file_sofar)) %>%
    tibble::as_data_frame() %>%
    dplyr::mutate_at(vars(3:ncol(.)), as.numeric) %>%
    {.}
  print(names(df_sofar))
  df_now = readr::read_csv(sprintf("~/Dropbox/ROCIS/DataBySensor/PurpleAir/download/%s/%s", now_date, f)) %>%
    tibble::as_data_frame() %>%
    dplyr::mutate_at(vars(3:ncol(.)), as.numeric) %>%
    {.}
  print(names(df_now))
  df = df_sofar %>%
    dplyr::bind_rows(df_now) %>%
    dplyr::group_by(`entry_id`) %>%
    slice(1) %>%
    dplyr::ungroup() %>%
    {.}
  if ("X11" %in% names(df)) {
    df <- df %>%
      dplyr::select(-`X11`) %>%
      {.}
  }
  print(names(df))
  file_out = gsub(prev_date, now_date, file_sofar)
  df %>%
    readr::write_csv(sprintf("~/Dropbox/ROCIS/DataBySensor/PurpleAir/sofar/%s", file_out))
}

## remove old "sofar" files
to_remove = list.files(path = "~/Dropbox/ROCIS/DataBySensor/PurpleAir/sofar/", pattern=sprintf("%s %s.csv", start_date, prev_date))

head(to_remove)
length(to_remove)

for (f in to_remove) {
  file.remove(sprintf("~/Dropbox/ROCIS/DataBySensor/PurpleAir/sofar/%s", f))
}

files = list.files(path="~/Dropbox/ROCIS/DataBySensor/PurpleAir/sofar",
                   pattern = ".csv")

head(files)
length(files)

summaryDf = data.frame(filename=files)
summaryDf <- summaryDf %>%
  dplyr::mutate(nameOnMap = substr(filename, 1, regexpr(" \\([0-9]", filename) - 1),
                lat = substr(filename, regexpr("\\([0-9]{2}", filename) + 1, regexpr("-", filename) - 2),
                lng = substr(filename, regexpr("-[0-9]{2}", filename), regexpr("[0-9]{3}\\)", filename) - 1),
                filetype = ifelse(grepl(" Primary ", filename, fixed=TRUE), "Primary", "Secondary")
                ) %>%
  {.}
head(summaryDf)

summaryDf <- summaryDf %>%
  ## manually change BCD nameOnMap, to prevent duplicate when joining with nameOnMap
  dplyr::mutate(nameOnMap = ifelse(grepl("40.454750648551645 -80.0105800238037", `filename`, fixed=TRUE),
                                   paste0(`nameOnMap`, "_I"),
                                   ifelse(grepl("40.45465731046842 -80.010823858197",`filename`, fixed=TRUE),
                                         paste0(`nameOnMap`, "_O"), `nameOnMap`))) %>%
  ## manually change RM nameOnMap, to prevent duplicate when joining with nameOnMap
  dplyr::mutate(nameOnMap = ifelse(grepl("40.31064239594321 -79.73376720029967", `filename`, fixed=TRUE),
                                   paste0(`nameOnMap`, "_I"),
                            ifelse(grepl("40.31031392930895 -79.73370936761796",`filename`, fixed=TRUE),
                                   paste0(`nameOnMap`, "_O"), `nameOnMap`))) %>%
  {.}

summaryDf %>%
  readr::write_csv("~/Dropbox/ROCIS/DataBySensor/PurpleAir/filename_parsing.csv")

summaryDf %>%
  dplyr::group_by(`nameOnMap`, `filetype`) %>%
  dplyr::filter(n()>1)

dfloc =
  readxl::read_excel("input/PurpleAir tracker_8-4-18_Yujie.xlsx", sheet=1) %>%
  as.data.frame() %>%
  dplyr::select(`NAME ON MAP`, `HOME ID`, `IOR`) %>%
  dplyr::rename(`nameOnMap`=`NAME ON MAP`) %>%
  dplyr::mutate(`nameOnMap`=ifelse(`nameOnMap`=="Northside", paste0(`nameOnMap`, "_", `IOR`), `nameOnMap`)) %>%
  dplyr::mutate(`nameOnMap`=ifelse(`nameOnMap`=="North Huntingdon", paste0(`nameOnMap`, "_", `IOR`), `nameOnMap`)) %>%
  ## sensor name on map doesn't correspond to filename nameOnMap field
  dplyr::mutate(`nameOnMap`=ifelse(`nameOnMap`=="Bethel Park BXJ 2", "Bethel Park BXJ2", `nameOnMap`)) %>%
  dplyr::filter(!is.na(`nameOnMap`)) %>%
  ## na.omit() %>%
  {.}
(dfloc)

df_hash =
  readr::read_csv("input/id_hashing.csv") %>%
  dplyr::rename(`HOME ID`=`home_id_standard`) %>%
  {.}
head(df_hash)

df_neighbor =
  readr::read_csv("input/ROCIS  LCMP Participants by Cohort_03-15-2016.csv") %>%
  dplyr::arrange(`HOME ID CORRECT`, `ROUND`) %>%
  dplyr::select(`HOME ID CORRECT`, `NEIGHBORHOOD`) %>%
  dplyr::group_by(`HOME ID CORRECT`) %>%
  dplyr::slice(n()) %>%
  dplyr::rename(`HOME ID`=`HOME ID CORRECT`) %>%
  {.}

summaryDf %>%
  dplyr::left_join(dfloc, by="nameOnMap") %>%
  dplyr::left_join(df_hash, by="HOME ID") %>%
  dplyr::left_join(df_neighbor, by="HOME ID") %>%
  readr::write_csv("~/Dropbox/ROCIS/DataBySensor/PurpleAir/filename_mapname_ior_hash.csv")

summaryByType <- function(fileType) {
  byTypeFiles = summaryDf %>%
    dplyr::filter(filetype == fileType) %>%
    .$filename
  acc_summary_byType = NULL
  for (f in byTypeFiles) {
    print(f)
    ## changed to original parsing method, seems not affecting the summary result
    df = readr::read_csv(sprintf("~/Dropbox/ROCIS/DataBySensor/PurpleAir/sofar/%s", f)) %>%
      as.data.frame() %>%
      dplyr::select(-`entry_id`) %>%
      {.}
    startTime = min(df$created_at)
    endTime = max(df$created_at)
    summary_individual = summaryUtil::summary_table(df) %>%
      dplyr::mutate(`filename`=f,
                    `start_time`=startTime,
                    `end_time`=endTime) %>%
      {.}
    acc_summary_byType = rbind(acc_summary_byType, summary_individual)
  }
  acc_summary_byType %>%
    readr::write_csv(sprintf("~/Dropbox/ROCIS/DataBySensor/PurpleAir/stat_summary_%s.csv", fileType))
}

## summary of primary file and secondary file
summaryByType("Primary")

summaryByType("Secondary")

## lookup home id from nameOnMap
nameOnMap_ID = dfloc %>%
  dplyr::select(`nameOnMap`, `HOME ID`) %>%
  dplyr::group_by(`nameOnMap`, `HOME ID`) %>%
  slice(1)
for (fileType in c("Primary", "Secondary")) {
  readr::read_csv(sprintf("~/Dropbox/ROCIS/DataBySensor/PurpleAir/stat_summary_%s.csv", fileType)) %>%
    dplyr::left_join(summaryDf, by="filename") %>%
    dplyr::left_join(dfloc, by="nameOnMap") %>%
    readr::write_csv(sprintf("~/Dropbox/ROCIS/DataBySensor/PurpleAir/summary_%s.csv", fileType))
}

## print file location lookup for selecting indoor outdoor files
## readr::read_csv(sprintf("~/Dropbox/ROCIS/DataBySensor/PurpleAir/summary_%s.csv", fileType)) %>%
##   dplyr::select(`filename`, `filetype`, `IOR`) %>%
##     distinct(`filename`, `IOR`) %>%
##     print(n=40)
## get outdoor file names
output_to_plot <- function (fileType, plot_loc, plot_loc_label, summaryDf, dfloc, df_hash, df_neighbor) {
  io_filenames =
    readr::read_csv(sprintf("~/Dropbox/ROCIS/DataBySensor/PurpleAir/summary_%s.csv", fileType)) %>%
    dplyr::select(`filename`, `filetype`, `IOR`) %>%
    dplyr::filter(`IOR` == plot_loc_label, `filetype`==fileType) %>%
    distinct(`filename`)
  io_filenames <-
    io_filenames %>%
    dplyr::left_join(summaryDf, by="filename") %>%
    dplyr::left_join(dfloc, by="nameOnMap") %>%
    dplyr::left_join(df_hash, by="HOME ID") %>%
    {.}
  ## read all outdoor files into a data frame
  acc_df = NULL
  for (f in io_filenames$filename) {
    print(f)
    ## ## changed to original parsing method, seems not affecting the summary result
    df = readr::read_csv(sprintf("~/Dropbox/ROCIS/DataBySensor/PurpleAir/sofar/%s", f)) %>%
      as.data.frame() %>%
      dplyr::select(-`entry_id`) %>%
      ## remove duplicate record
      dplyr::group_by_all() %>%
      dplyr::slice(1) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(`filename`=f) %>%
      {.}
    acc_df = rbind(acc_df, df)
  }
  print(head(summaryDf))
  acc_df <-
    acc_df %>%
    dplyr::left_join(summaryDf, by="filename") %>%
    dplyr::left_join(dfloc, by="nameOnMap") %>%
    dplyr::left_join(df_hash, by="HOME ID") %>%
    dplyr::left_join(df_neighbor, by="HOME ID") %>%
    dplyr::mutate(`labeling`=sprintf("%.0f_%s_%s", `hashing`, `IOR`, `NEIGHBORHOOD`)) %>%
    dplyr::mutate(`name_label`=sprintf("%s (filename: %s)", `HOME ID`, `filename`)) %>%
    dplyr::select(-`filename`, -`lat`, -`lng`, -`filetype`, -`nameOnMap`, -`HOME ID`) %>%
    {.}
  ## no duplicates
  ## acc_df %>%
  ##   dplyr::group_by_all() %>%
  ##   dplyr::filter(n()>1) %>%
  ##   head()
  variables = names(acc_df)
  variables <- variables[variables != "created_at"]
  variables <- variables[variables != "labeling"]
  variables <- variables[variables != "name_label"]
  variables <- variables[variables != "IOR"]
  variables <- variables[variables != "NEIGHBORHOOD"]
  variables <- variables[variables != "PM10_CF_1_ug_per_m3"]
  variables <- variables[variables != "PM1.0_CF_1_ug_per_m3"]
  variables <- variables[variables != "hashing"]
  for (v in variables) {
    v_in_filename = gsub("/", "_per_", v)
    toPlot =
      acc_df %>%
      dplyr::select(one_of("created_at", v, "labeling")) %>%
      tidyr::spread(labeling, !!rlang::sym(v)) %>%
      {.}
    toPlot %>%
      readr::write_csv(sprintf("~/Dropbox/ROCIS/DataBySensor/PurpleAir/%s_cmp_plots/raw_data/var_%s.csv", plot_loc, v_in_filename))
  }
  return(acc_df)
}

acc_df <- output_to_plot(fileType="Secondary", plot_loc="outdoor", plot_loc_label="O", summaryDf=summaryDf, dfloc=dfloc, df_hash=df_hash, df_neighbor=df_neighbor)

acc_df <- output_to_plot(fileType="Secondary", plot_loc="indoor", plot_loc_label="I", summaryDf=summaryDf, dfloc=dfloc, df_hash=df_hash, df_neighbor=df_neighbor)

## ## create files for side-by-side dylos and Purple Air
## labels = unique(acc_df$`name_label`)
## for (label in labels) {
##   print(label)
##   home = substr(label, start=1, stop=(gregexpr(pattern =' ',label)[[1]][[1]]) - 1)
##   purpleair_filename = substr(label, start=(gregexpr(pattern =': ',label)[[1]][[1]]) + 2, stop=nchar(label) - 1)
##   print(purpleair_filename)
##   print(home)
##   dylos_file = readr::read_csv(sprintf("~/Dropbox/ROCIS/DataBySensor/Dylos/concat_gen_spe/round_all/%s_O_-.csv", home)) %>%
##     as.data.frame() %>%
##     {.}
##   intervention_filename = sprintf("~/Dropbox/ROCIS/DataBySensor/Dylos/concat_gen_spe/round_intervention/%s_O_-.csv", home)
##   if (file.exists(intervention_filename)) {
##     dylos_file_intervention = readr::read_csv(intervention_filename) %>%
##       as.data.frame() %>%
##       {.}
##     dylos_file <- dylos_file %>%
##       dplyr::bind_rows(dylos_file_intervention) %>%
##       dplyr::group_by(`Date/Time`) %>%
##       slice(1) %>%
##       dplyr::ungroup() %>%
##       {.}
##   }
##   dylos_file <- dylos_file %>%
##     ## convert local to UTC time
##     dplyr::mutate(`Date/Time`=as.character(`Date/Time`)) %>%
##     dplyr::mutate(`Date/Time`=as.POSIXct(`Date/Time`, format="%Y-%m-%d %H:%M:%S",tz="America/New_York")) %>%
##     dplyr::mutate(`Date/Time`=format(`Date/Time`, tz="UTC", usetz = TRUE)) %>%
##     dplyr::mutate(st_dt = as.character(`Date/Time`)) %>%
##     na.omit() %>%
##     dplyr::mutate(quarter = paste0(substr(st_dt, 1, 14),
##                                  sprintf("%02d", (as.integer(substr(st_dt, 15, 16)) %/% 15) * 15),
##                                  substr(st_dt, 17, 23)))%>%
##     dplyr::select(-`st_dt`, -`Date/Time`) %>%
##     dplyr::group_by(`quarter`) %>%
##     dplyr::summarise_all(mean) %>%
##     dplyr::ungroup() %>%
##     dplyr::mutate(`Small_minus_Large` = `Small` - `Large`) %>%
##     dplyr::select(-`Small`) %>%
##     {.}
##   print(head(dylos_file))
##   ## dylos_file %>%
##   ##   readr::write_csv("~/Dropbox/ROCIS/DataBySensor/PurpleAir/temp/dylos_file.csv")
##   purpleair_file = readr::read_csv(sprintf("~/Dropbox/ROCIS/DataBySensor/PurpleAir/download/%s/%s",
##                                            download_date, purpleair_filename)) %>%
##     as.data.frame() %>%
##     dplyr::select(`created_at`, `0.5um/dl`, `2.5um/dl`) %>%
##     dplyr::mutate(st_dt = `created_at`) %>%
##     dplyr::mutate(quarter = paste0(substr(st_dt, 1, 14),
##                                   sprintf("%02d", (as.integer(substr(st_dt, 15, 16)) %/% 15) * 15),
##                                   substr(st_dt, 17, 23)))%>%
##     dplyr::select(-`st_dt`, -`created_at`) %>%
##     dplyr::group_by(`quarter`) %>%
##     dplyr::summarise_all(mean) %>%
##     dplyr::ungroup() %>%
##     ## 0.01cf / 0.1L = 2.83168466
##     dplyr::mutate(`0.5um/100thCubicFoot` = `0.5um/dl` * 2.83168466,
##                   `2.5um/100thCubicFoot` = `2.5um/dl` * 2.83168466) %>%
##     dplyr::select(-`0.5um/dl`, -`2.5um/dl`) %>%
##     {.}
##   print(head(purpleair_file))
##   ## purpleair_file %>%
##   ##   readr::write_csv("~/Dropbox/ROCIS/DataBySensor/PurpleAir/temp/purpleair_file.csv")
##   data_file = purpleair_file %>%
##     dplyr::inner_join(dylos_file, by="quarter") %>%
##     {.}
##   data_file %>%
##     dplyr::select(quarter, `Small_minus_Large`, `0.5um/100thCubicFoot`) %>%
##     readr::write_csv(sprintf("~/Dropbox/ROCIS/DataBySensor/PurpleAir/dylos_purpleair/%s_O_small.csv", home))
##   data_file %>%
##     dplyr::select(quarter, `Large`, `2.5um/100thCubicFoot`) %>%
##     readr::write_csv(sprintf("~/Dropbox/ROCIS/DataBySensor/PurpleAir/dylos_purpleair/%s_O_large.csv", home))
## }
