# Load packages
# these are all the packages used for the markdown report
packages <- c("corrplot", "cowplot", "devtools", "DT", "dygraphs", "egg",
  "ggplot2", "Hmisc", "ipayipi", "kableExtra", "khroma", "leaflet",
  "lubridate", "mice", "sf", "viridisLite"
)

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}
# ipayipi is installed from github
devtools::install_github("pauljeco/ipayipi", force = TRUE)

# load package --- for now only ipayipi
library(ipayipi)

# define the working directory ---
# must be reletive to the console/terminal working directory
wd <- "ipayipi_vignettes/rainfall_eg"

pipe_house <- ipip_house(wd)

rmarkdown::render(file.path("ipayipi_vignettes/rainfall_eg/reports/markdown/",
    "rainfall_pipe_eg.rmd"
  ), output_dir = "ipayipi_vignettes/rainfall_eg/reports"
)
p <- dta_availability(pipe_house)
plotly::ggplotly(p, tooltip = "text")
