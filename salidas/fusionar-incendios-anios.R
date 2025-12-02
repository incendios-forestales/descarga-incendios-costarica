# ===============================================================
# Unir todos los GPKG de incendios del directorio actual
# ===============================================================

## === 1. CONFIGURACIÓN: nombres de archivos de salida ========
output_gpkg <- "incendios_clima_cr_2001-2024.gpkg" # archivo final GPKG

## === 2. CARGAR LIBRERÍAS =====================================
suppressPackageStartupMessages({
  library(dplyr)   # bind_rows
  library(sf)      # leer/escribir GeoJSON GPKG
})

## === 3. PROCESAR GPKG ====================================
message("\n🔍 Buscando archivos GPKG…")
geo_files <- list.files(pattern = "\\.gpkg$", ignore.case = TRUE) |> sort()

if (length(geo_files) == 0) {
  stop("❌ No se encontraron archivos GPKG en el directorio.")
}

message("🌐 Leyendo y combinando GPKG:")
geo_list <- lapply(geo_files, function(f) {
  message("   • ", f)
  st_read(f, quiet = TRUE)  # cada archivo como objeto sf
})

geo_combined <- do.call(rbind, geo_list)

message("💾 Escribiendo archivo combinado → ", output_gpkg)
st_write(geo_combined, output_gpkg, delete_dsn = TRUE, quiet = TRUE)
message("✅ GPKG consolidado creado con éxito.")

## === 4. FIN =================================================
message("\n🎉 Proceso completado satisfactoriamente.")
