# Databricks notebook source
import os

# Usamos la nueva variable que pide Kaggle en tu imagen
os.environ['KAGGLE_USERNAME'] = "brandonarboleda" # Tu usuario (basado en tus datos)
os.environ['KAGGLE_API_TOKEN'] = "KGAT_b00ec0de6e05a8720cbe962646031e35"

print("Autenticación configurada. Listos para cargar data no estructurada.")

# COMMAND ----------

# MAGIC %sh
# MAGIC # Limpiar e intentar la descarga con las variables de entorno heredadas de Python
# MAGIC rm -rf /tmp/plants_raw /tmp/plants_unzipped
# MAGIC mkdir -p /tmp/plants_raw /tmp/plants_unzipped
# MAGIC
# MAGIC # Descarga directa (usando las variables de entorno de arriba)
# MAGIC kaggle competitions download -c plant-pathology-2021-fgvc8 -p /tmp/plants_raw
# MAGIC
# MAGIC # Descomprimir y mover al volumen
# MAGIC unzip -q /tmp/plants_raw/*.zip -d /tmp/plants_unzipped
# MAGIC mkdir -p /Volumes/workspace/bronze/yeik/plants/
# MAGIC cp -r /tmp/plants_unzipped/* /Volumes/workspace/bronze/yeik/plants/
# MAGIC
# MAGIC # Verificar qué archivos quedan en /tmp después del proceso
# MAGIC echo "Archivos restantes en /tmp/plants_raw:"
# MAGIC ls -lh /tmp/plants_raw
# MAGIC
# MAGIC echo "Archivos restantes en /tmp/plants_unzipped:"
# MAGIC ls -lh /tmp/plants_unzipped
# MAGIC
# MAGIC echo "¡Carga completada!"

# COMMAND ----------

ls /Volumes/workspace/bronze/yeik/plants/

# COMMAND ----------

# MAGIC %sh
# MAGIC # 1. Crear la carpeta que falta
# MAGIC mkdir -p /Volumes/workspace/bronze/yeik/plants/new_uploads/
# MAGIC
# MAGIC # 2. Simular la llegada de nuevos datos (Copiamos 5 imágenes al azar)
# MAGIC # Esto es para que el SQL tenga algo que leer y no falle
# MAGIC cp $(ls /Volumes/workspace/bronze/yeik/plants/train_images/*.jpg | head -n 5) /Volumes/workspace/bronze/yeik/plants/new_uploads/
# MAGIC
# MAGIC echo "Carpeta creada y datos de prueba cargados."