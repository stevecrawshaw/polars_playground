pacman::p_load(tidyverse,
               duckplyr,
               conflicted)

conflict_prefer("filter", "duckplyr")


ld <- duckplyr_df_from_file(
    path = "../../python_projects/polars_playground/data/2024-01_sds011.csv",
    table_function = "read_csv",
    options = list(
        delim = ";",
        header = TRUE,
        ignore_errors = TRUE
    )
)


ld_filtered <- ld |> 
    select(sensor_id, lat, lon, P1, P2, timestamp) |> 
    filter(lat >= 51.2 & lat <= 51.6 & lon >= -3.0 & lon <= -2.18) |> 
    # between clause doesn't work in duckplyr
    mutate(P1 = as.numeric(P1),
           P2 = as.numeric(P2),
           date_time_hr = ceiling_date(timestamp, "hour")) |> # need to create the time grouping var in mutate clause - won't work in summarise .by()
    summarise(pm10 = mean(P1, na.rm = TRUE),
              pm25 = mean(P2, na.rm = TRUE), 
              .by = c(sensor_id, date_time_hr, lat, lon)) |> 
    explain()

    glimpse(ld_filtered)

x <- Sys.time()
as.Date(x)
# 
# between(lat, 51.2, 51.6) & between(lon, -3.0, -2.18)
# lat >= 51.2 AND lat <= 51.6 AND lon >= -3.0 AND lon <= -2.18