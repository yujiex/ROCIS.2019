library("dplyr")
library("readr")

setwd("../DataBySensor/PurpleAir")

dfsummary = readr::read_csv("filename_mapname_ior_hash.csv") %>%
  dplyr::filter(`filetype`=="Secondary",
                !is.na(`IOR`)) %>%
  dplyr::group_by(`HOME ID`) %>%
  dplyr::filter(n() == 2) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(`header`=sprintf("%s_%s_%s", `hashing`, `IOR`, `NEIGHBORHOOD`)) %>%
  {.}

head(dfsummary)

homes = unique(dfsummary$`header`)
hashes = as.character(unique(dfsummary$`hashing`))

homes
hashes

get_to_plot <- function(location) {
  acc_loc = NULL
  ## for (size in c(0.3)) {
  for (size in c(0.3, 0.5, 1.0, 2.5)) {
    print(size)
    df_size = readr::read_csv(sprintf("%s_cmp_plots/15min_avg/%s_var_%.1fum_per_dl.csv", location, location, size)) %>%
      dplyr::select(one_of(c("created_at", homes))) %>%
      ## dplyr::rename_at(vars(-starts_with("created")), funs(sprintf("%s_%s", ., size))) %>%
      tidyr::gather(`header`, `value`, -starts_with("created")) %>%
      na.omit() %>%
      dplyr::mutate(`particleSize`=size) %>%
      {.}
    acc_loc = rbind(acc_loc, df_size)
  }
  return(acc_loc)
}

df1 = get_to_plot(location="indoor")
df2 = get_to_plot(location="outdoor")

head(df1)
head(df2)

df3 = df1 %>%
  dplyr::bind_rows(df2) %>%
  {.}
  
head(df3)
tail(df3)
nrow(df3)

df1 %>%
  readr::write_csv(sprintf("indoor_outdoor_plots/%s.csv", location))

loc_to_plot = df3 %>%
  dplyr::mutate(`header_with_size`=sprintf("%s_%.1f", header, particleSize)) %>%
  dplyr::select(-`particleSize`, -`header`) %>%
  tidyr::spread(`header_with_size`, `value`) %>%
  {.}

head(loc_to_plot)

class(hashes)

for (h in hashes) {
  loc_to_plot %>%
    dplyr::select(`created_at`, starts_with(h)) %>%
    dplyr::mutate(`created_at`=as.character(`created_at`)) %>%
    tidyr::gather(`header`, `value`, starts_with(h)) %>%
    na.omit() %>%
    tidyr::spread(`header`, `value`) %>%
    readr::write_csv(sprintf("indoor_outdoor_plots/%s.csv", h), na="")
}

## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
## following for just indoor
## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##

dfsummary = readr::read_csv("filename_mapname_ior_hash.csv") %>%
  dplyr::filter(`filetype`=="Secondary",
                !is.na(`IOR`)) %>%
  dplyr::filter(IOR == "I") %>%
  dplyr::ungroup() %>%
  dplyr::mutate(`header`=sprintf("%s_%s_%s", `hashing`, `IOR`, `NEIGHBORHOOD`)) %>%
  {.}

homes = unique(dfsummary$`header`)
hashes = as.character(unique(dfsummary$`hashing`))

homes
hashes

df1 = get_to_plot(location="indoor")

loc_to_plot = df1 %>%
  dplyr::mutate(`header_with_size`=sprintf("%s_%.1f", header, particleSize)) %>%
  dplyr::select(-`particleSize`, -`header`) %>%
  tidyr::spread(`header_with_size`, `value`) %>%
  {.}

head(loc_to_plot)

class(hashes)

for (h in hashes) {
  loc_to_plot %>%
    dplyr::select(`created_at`, starts_with(h)) %>%
    dplyr::mutate(`created_at`=as.character(`created_at`)) %>%
    tidyr::gather(`header`, `value`, starts_with(h)) %>%
    na.omit() %>%
    tidyr::spread(`header`, `value`) %>%
    readr::write_csv(sprintf("indoor_by_home_plots/%s.csv", h), na="")
}
