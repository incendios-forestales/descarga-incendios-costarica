#!/usr/bin/env Rscript

# Extrae variables ERA5(-Land) para puntos (Costa Rica) en la fecha acq_date
# Salida: GPKG con columnas nuevas de clima (diarias por fecha del punto)

suppressPackageStartupMessages({
  library(dplyr)
  library(lubridate)
  library(httr)
  library(jsonlite)
  library(sf)
  library(terra)
  library(KrigR)
})

# ---------------------------
# Utilidades CLI
# ---------------------------
.get_arg_val <- function(args, long_flag, key = NULL) {
  val <- NA_character_
  if (length(args) >= 2 && any(args == long_flag)) {
    idx <- which(args == long_flag)[1]
    if (idx < length(args)) val <- args[idx + 1]
  }
  if (is.na(val) && !is.null(key)) {
    kv <- grep(paste0("^", key, "="), args, value = TRUE)
    if (length(kv)) val <- sub(paste0("^", key, "="), "", kv[1])
  }
  val
}

# args <- commandArgs(trailingOnly = TRUE)

# input_gpkg <- .get_arg_val(args, "--input",      "input")
# date_field <- .get_arg_val(args, "--date-field", "date_field")
# output_gpkg <- .get_arg_val(args, "--output",    "output")
# ys <- .get_arg_val(args, "--year-start", "year_start")
# ye <- .get_arg_val(args, "--year-end",   "year_end")
# year_start <- if (!is.na(ys)) as.integer(ys) else NA_integer_
# year_end   <- if (!is.na(ye)) as.integer(ye) else NA_integer_

input_gpkg  <- "datos/incendios-cr-modis.gpkg"
date_field  <- "acq_date"
output_gpkg <- "salidas/incendios-cr-2023_2024.gpkg"
ys <- "2023"
ye <- "2024"
year_start <- if (!is.na(ys)) as.integer(ys) else NA_integer_
year_end   <- if (!is.na(ye)) as.integer(ye) else NA_integer_

if (is.na(input_gpkg) || !file.exists(input_gpkg)) {
  stop("Debe indicar --input <ruta.gpkg> (existente).")
}
if (is.na(date_field)) {
  stop("Debe indicar --date-field <nombre_columna_fecha> (ej. acq_date).")
}
if (is.na(output_gpkg)) {
  # por defecto escribe en 'salidas/<nombre_base>-era5.gpkg'
  base_dir <- getwd()
  dir.create(file.path(base_dir, "salidas"), showWarnings = FALSE, recursive = TRUE)
  bn <- sub("\\.gpkg$", "", basename(input_gpkg))
  output_gpkg <- file.path(base_dir, "salidas", paste0(bn, "-era5.gpkg"))
}

readRenviron(".Renviron")

# ---------------------------
# Configuración de variables ERA5(-Land)
# ---------------------------
DATASET <- "reanalysis-era5-single-levels"
TYPE    <- "reanalysis"

AGGREGATE_FUNCTIONS <- c("mean") # precip cambia a sum automáticamente

VARIABLES <- c(
  "2m_temperature",
  "total_precipitation",
  "10m_u_component_of_wind",
  "10m_v_component_of_wind"
)

COLUMN_PREFIXES <- c(
  "TEMPERATURE_2M_ACQ_DATE",
  "PRECIPITATION_ACQ_DATE",
  "10M_U_COMPONENT_OF_WIND_ACQ_DATE",
  "10M_V_COMPONENT_OF_WIND_ACQ_DATE"
)

# Reglas de conversión de unidades (a unidades “humanas”)
unit_conversion_rules <- list(
  "2m_temperature"      = function(x) x - 273.15, # K → °C
  "total_precipitation" = function(x) x * 1000    # m → mm
  # u/v en m/s ya está bien
)

.is_precip <- function(var) grepl("precip", var, ignore.case = TRUE)
.map_agg <- function(var, agg) {
  if (.is_precip(var) && agg == "mean") list(suffix = "SUM", fun = "sum") else list(suffix = toupper(agg), fun = agg)
}

# ---------------------------
# Lectura de datos
# ---------------------------
message("Leyendo puntos desde: ", input_gpkg)
pts_sf <- st_read(input_gpkg, quiet = TRUE)
if (!inherits(pts_sf, "sf")) stop("El archivo no es un 'sf' válido.")
if (!"POINT" %in% unique(st_geometry_type(pts_sf))) {
  stop("Se esperan geometrías de tipo POINT en el GPKG de entrada.")
}

# Asegura CRS = 4326
if (is.na(st_crs(pts_sf))) stop("El GPKG no tiene CRS. Asigna uno antes de ejecutar (se espera WGS84).")
pts_sf <- st_transform(pts_sf, 4326)

# Fecha del punto
if (!date_field %in% names(pts_sf)) {
  stop("No existe la columna de fecha '", date_field, "' en el GPKG.")
}
# Normaliza fecha a Date (si viene POSIXct/char)
if (inherits(pts_sf[[date_field]], "POSIXt")) {
  pts_sf[[date_field]] <- as.Date(pts_sf[[date_field]])
} else if (is.numeric(pts_sf[[date_field]])) {
  # por si viniera epoch en segundos/milisegundos
  # heurística simple: si es muy grande, asume milisegundos
  if (median(pts_sf[[date_field]], na.rm = TRUE) > 1e10) {
    pts_sf[[date_field]] <- as.Date(as.POSIXct(pts_sf[[date_field]]/1000, origin = "1970-01-01", tz = "UTC"))
  } else {
    pts_sf[[date_field]] <- as.Date(as.POSIXct(pts_sf[[date_field]], origin = "1970-01-01", tz = "UTC"))
  }
} else {
  pts_sf[[date_field]] <- as.Date(pts_sf[[date_field]])
}

# Filtra filas con fecha válida
pts_sf <- pts_sf |> filter(!is.na(.data[[date_field]]))
if (nrow(pts_sf) == 0) stop("No hay filas con fecha válida en '", date_field, "'.")

# Filtrar por year_start / year_end si se indicaron
if (!is.na(year_start) || !is.na(year_end)) {
  # Completar extremos si alguno falta
  if (is.na(year_start)) year_start <- lubridate::year(min(pts_sf[[date_field]], na.rm = TRUE))
  if (is.na(year_end))   year_end   <- lubridate::year(max(pts_sf[[date_field]], na.rm = TRUE))
  if (year_start > year_end) stop("year_start no puede ser mayor que year_end.")
  
  pts_sf <- pts_sf |>
    dplyr::filter(dplyr::between(lubridate::year(.data[[date_field]]), year_start, year_end))
  
  if (nrow(pts_sf) == 0) stop(sprintf("No hay registros entre %d y %d.", year_start, year_end))
}

# Etiqueta por rango de fechas (años)
if (is.na(year_start) || is.na(year_end)) {
  yr_min <- lubridate::year(min(pts_sf[[date_field]], na.rm = TRUE))
  yr_max <- lubridate::year(max(pts_sf[[date_field]], na.rm = TRUE))
  if (is.na(year_start)) year_start <- yr_min
  if (is.na(year_end))   year_end   <- yr_max
}
label <- sprintf("%d-%d", year_start, year_end)

# Directorios
dir_base <- getwd()
dir_tmp  <- file.path(dir_base, "tmp", paste0("CR-", label))
dir_out  <- dirname(output_gpkg)
dir.create(dir_tmp, recursive = TRUE, showWarnings = FALSE)
dir.create(dir_out, recursive = TRUE, showWarnings = FALSE)

# BBox de los puntos (con pequeño buffer)
bb_raw <- st_bbox(pts_sf)
buf <- 0.5
xmin <- as.numeric(bb_raw["xmin"]) - buf
xmax <- as.numeric(bb_raw["xmax"]) + buf
ymin <- as.numeric(bb_raw["ymin"]) - buf
ymax <- as.numeric(bb_raw["ymax"]) + buf

# Crear un SpatExtent (terra) para pasarlo a KrigR
ext_bb <- terra::ext(xmin, xmax, ymin, ymax)

# (opcional) si prefieres sf:
# bb_sf <- sf::st_as_sfc(sf::st_bbox(c(xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax), crs=4326))

# SpatVector para extracción
xy <- st_coordinates(pts_sf)
pts_sv <- terra::vect(
  data.frame(id = seq_len(nrow(pts_sf)), Lon = xy[,1], Lat = xy[,2]),
  geom = c("Lon", "Lat"),
  crs  = "EPSG:4326"
)

# Prepara columnas de salida
for (i in seq_along(VARIABLES)) {
  var  <- VARIABLES[i]
  pref <- COLUMN_PREFIXES[i]
  for (agg in AGGREGATE_FUNCTIONS) {
    m <- .map_agg(var, agg)
    colname <- paste0(pref, "_", m$suffix)
    if (!colname %in% names(pts_sf)) pts_sf[[colname]] <- NA_real_
  }
}
# También las derivadas de viento
spd_col <- "10M_WIND_SPEED_ACQ_DATE_MEAN"
dir_col <- "10M_WIND_DIRECTION_ACQ_DATE_MEAN"
if (!spd_col %in% names(pts_sf)) pts_sf[[spd_col]] <- NA_real_
if (!dir_col %in% names(pts_sf)) pts_sf[[dir_col]] <- NA_real_

# ---------------------------
# Descarga y extracción por AÑO (para no cargar miles de capas a la vez)
# ---------------------------
u_days <- sort(unique(as.Date(pts_sf[[date_field]])))
years  <- sort(unique(lubridate::year(u_days)))

for (i in seq_along(VARIABLES)) {
  var  <- VARIABLES[i]
  pref <- COLUMN_PREFIXES[i]
  
  for (agg in AGGREGATE_FUNCTIONS) {
    m <- .map_agg(var, agg)
    base_col <- paste0(pref, "_", m$suffix)
    total_filled <- 0L
    
    message(sprintf("→ ERA5 %s | agregación diaria: %s → columna: %s", var, m$fun, base_col))
    
    for (yy in years) {
      days_y <- u_days[lubridate::year(u_days) == yy]
      if (!length(days_y)) next
      
      date_start <- min(days_y)
      date_stop  <- max(days_y)
      
      # Descarga diaria agregada de ese año (o subrango dentro del año)
      ras <- tryCatch({
        KrigR::CDownloadS(
          Variable    = var,
          DataSet     = DATASET,   # <- ahora usamos solo ERA5 single levels
          Type        = TYPE,
          DateStart   = format(date_start, "%Y-%m-%d"),
          DateStop    = format(date_stop,  "%Y-%m-%d"),
          TZone       = "UTC",
          TResolution = "day",
          FUN         = m$fun,
          TStep       = 1,
          Extent      = ext_bb,
          Dir         = dir_tmp,
          FileName    = sprintf("%s_%s_CR_diaria_%d", var, m$fun, yy),
          API_User    = Sys.getenv("CDSAPI_USER"),
          API_Key     = Sys.getenv("CDSAPI_KEY"),
          Cores       = as.integer(Sys.getenv("KRIGR_CORES", "1"))
        )
      }, error = function(e) {
        warning(sprintf("  ! Falló descarga %s (%s) año %d: %s (se omite).", var, m$fun, yy, e$message))
        return(NULL)
      })
      if (is.null(ras)) next
      terra::crs(ras) <- "EPSG:4326"
      
      # Conversión de unidades si aplica
      if (!is.null(unit_conversion_rules[[var]]) && is.function(unit_conversion_rules[[var]])) {
        ras <- unit_conversion_rules[[var]](ras)
      }
      
      # Guardar para auditoría
      terra::writeRaster(
        ras,
        filename = file.path(dir_tmp, sprintf("%s_%s_CR_diaria_%d.tif", var, m$fun, yy)),
        datatype  = "FLT4S",
        overwrite = TRUE,
        NAflag    = -9999
      )
      
      # Fechas de las bandas
      ras_time <- tryCatch(terra::time(ras), error = function(e) NULL)
      if (!is.null(ras_time) && length(ras_time) == terra::nlyr(ras)) {
        ras_dates <- as.Date(ras_time)
      } else {
        ras_dates <- seq.Date(date_start, date_stop, by = "day")
        if (terra::nlyr(ras) != length(ras_dates)) {
          warning("  ! Nº de bandas no coincide con fechas esperadas; intento con seq.Date().")
          ras_dates <- ras_dates[seq_len(min(length(ras_dates), terra::nlyr(ras)))]
        }
      }
      
      # Filas de este año (puntos cuya fecha cae en yy)
      rows_y <- which(lubridate::year(pts_sf[[date_field]]) == yy)
      if (!length(rows_y)) next
      
      # Método de extracción (para precip "simple"; resto "bilinear")
      extract_method <- if (.is_precip(var)) "simple" else "bilinear"
      
      # Extrae por cada día de este año
      for (d in days_y) {
        idx <- match(as.Date(d), ras_dates)
        if (is.na(idx)) next
        
        rows_d <- rows_y[as.Date(pts_sf[[date_field]][rows_y]) == d]
        if (!length(rows_d)) next
        
        vals_df <- terra::extract(
          ras[[idx]],
          pts_sv[rows_d],
          method = extract_method
        )
        vals <- vals_df[[ setdiff(names(vals_df), "ID") ]]
        pts_sf[[base_col]][rows_d] <- vals
        total_filled <- total_filled + sum(!is.na(vals))
      }
    }
    
    message(sprintf("  ✓ %s (%s): valores asignados para %d registros.", var, m$suffix, total_filled))
  }
}

# ---------------------------
# Derivados de viento: velocidad y dirección (desde)
# ---------------------------
u_col <- "10M_U_COMPONENT_OF_WIND_ACQ_DATE_MEAN"
v_col <- "10M_V_COMPONENT_OF_WIND_ACQ_DATE_MEAN"

if (all(c(u_col, v_col) %in% names(pts_sf))) {
  u <- pts_sf[[u_col]]
  v <- pts_sf[[v_col]]
  
  # Velocidad (m/s)
  pts_sf[[spd_col]] <- sqrt(u^2 + v^2)
  
  # Dirección meteorológica "desde" (0=N, 90=E, 180=S, 270=O)
  # Fórmula estándar con u (este+) y v (norte+):
  # dir = atan2(-u, -v) en radianes -> grados [0,360)
  dir_deg <- (atan2(-u, -v) * 180 / pi) %% 360
  pts_sf[[dir_col]] <- dir_deg
}

# ---------------------------
# Guardar salida
# ---------------------------
message("Escribiendo salida en: ", output_gpkg)
if (file.exists(output_gpkg)) unlink(output_gpkg)

st_write(pts_sf, output_gpkg, driver = "GPKG", quiet = TRUE)
message("✓ Archivo GPKG generado con éxito.")

