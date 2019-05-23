library(leaflet)
library(htmlwidgets)

## source: https://github.com/rstudio/leaflet/blob/master/R/scalebar.R
addScaleBar <- function(map,
  position = c('topright', 'bottomright', 'bottomleft', 'topleft'),
  options = scaleBarOptions()) {

  options = c(options, list(position = match.arg(position)))
  invokeMethod(map, getMapData(map), 'addScaleBar', options)
}
scaleBarOptions <- function(maxWidth = 100, metric = TRUE, imperial = TRUE,
	updateWhenIdle = TRUE) {
	list(maxWidth=maxWidth, metric=metric, imperial=imperial,
		updateWhenIdle=updateWhenIdle)
}
df = read.csv('input/feed_info.csv')
df <- df[c("id", "latitude", "longitude")]
df <- df[complete.cases(df),]
write.csv(df, 'input/temp.csv')
m <- leaflet(df) %>% setView(lng = -80.01, lat = 40.44, zoom = 12)
m <- addScaleBar(m, position = "topright")
m <- addProviderTiles(m, providers$CartoDB.Positron)
m <- addCircleMarkers(m, lat=~latitude, lng=~longitude,
                      color = "blue",
                      fillOpacity = 0.5, radius = 5,
                      popup = ~as.character(id),
                      label = ~as.character(id))
## m
saveWidget(m, file="measure_loc.html", selfcontained=TRUE)
