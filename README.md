# 📊 MercadoPago - Data Pipeline para Value Props

Este proyecto desarrolla un pipeline de ingeniería de datos en Python para preparar un dataset que será utilizado en un modelo de Machine Learning de **MercadoPago**. El objetivo es predecir el orden óptimo de propuestas de valor (*value props*).

---

## 🧠 Objetivo

Construir un dataset final que contenga, para cada impresión (*print*) de la última semana:

- Si hubo clic (*tap*) o no.
- Cuántas veces el usuario vio esa *value prop* en las 3 semanas previas.
- Cuántos clics hizo sobre esa *value prop* en las 3 semanas previas.
- Cuántos pagos hizo relacionados con esa *value prop* en ese período.
- Cuánto dinero gastó en esa *value prop* en ese período.

---
 Decisiones Técnicas Tomadas
Durante la exploración y el desarrollo del pipeline de datos, se tomaron las siguientes decisiones clave para asegurar la calidad, eficiencia y escalabilidad del dataset final:

1. Expansión de columnas estructuradas:

Los archivos prints.json y taps.json contenían una columna event_data con información estructurada (tipo Struct). Esta fue expandida en columnas planas (position, value_prop, etc.) para facilitar el análisis y la construcción de features.

2. Estandarización temporal:

Se cambio el nombre de la columna de fecha (pay_date) por (day) en el dataframe del archivo pays.csv para que fuera igual que en todos los dataframes y se normalizo a un formato date, se generaron columnas auxiliares como week. Esto permitió aplicar filtros por semana y construir ventanas de tiempo móviles de manera consistente.

3. Limpieza de datos:
Se aplicó una función de limpieza uniforme que elimina duplicados y nulos, asegurando que los datos a procesar sean únicos y relevantes. Este paso fue especialmente importante para evitar sobrecontar impresiones, clics o pagos.

4. Identificación de clics (taps):

Para determinar si una impresión fue clickeada, se creó una clave única combinando day, user_id y value_prop. Esta clave fue usada para realizar un left join eficiente entre prints y taps, generando la variable binaria clicked.

5. Agregaciones históricas:

Se construyeron variables históricas para cada combinación (user_id, value_prop) en las 3 semanas previas a cada impresión de la última semana:

Cantidad de impresiones (n_prints_prev3w)

Cantidad de clics (n_taps_prev3w)

Cantidad de pagos (n_pays_prev3w)

Total gastado (sum_pays_prev3w)

Estas features permiten capturar el comportamiento pasado del usuario y serán claves para el modelo predictivo.

6. Modularización del script y legibilidad profesional:

El script feature_builder_polars.py fue diseñado en forma modular, utilizando funciones limpias y reutilizables para cada paso del proceso. Esto facilita el mantenimiento, testing y futura extensión del pipeline.

Las funciones clave implementadas son:

Función	Propósito
build_join_key(df): Crea una clave única combinando day, user_id, value_prop para detectar clics.

get_last_week_prints(prints):   	Filtra los prints que ocurrieron en la última semana del dataset.

mark_clicks(last_week_prints, taps):	Marca con 1 los prints que tuvieron un tap en la misma fecha.

get_historical_aggregates(...):	Calcula agregaciones históricas (prints, taps y pagos en 3 semanas previas).

build_training_dataset(...):	Combina todos los pasos anteriores para construir el dataset final.

main():	Ejecuta el pipeline completo, desde la carga hasta la escritura en formato Parquet.

7. Eficiencia y formato del output:

Se utilizó la librería Polars por su alto rendimiento en procesamiento de datos tabulares. El dataset final se guardó en formato Parquet, lo cual permite una compresión eficiente y una lectura rápida para tareas posteriores de modelado.


---
## 📂 Estructura del proyecto
```bash
MercadoPago/
├── src/
│ ├── data_loader_polars.py # Módulo para cargar y limpiar datos
│ ├── feature_builder_polars.py # Pipeline completo de construcción del dataset
├── data/ # Archivos de entrada (prints.json, taps.json, pays.csv)
├── notebooks/
│ ├── exploratory_analysis.ipynb # Cuaderno donde se realizo el analisis exploratorio de datos
│ ├── test_final_dataset.ipynb # Cuaderno donde se verifico el dataset final
├── output/
│ └── final_dataset.parquet # Dataset final generado
├── requirements.txt # Dependencias
└── README.md # Este archivo
```

---

## ⚙️ Requisitos

Python ≥ 3.9

Instalar las dependencias necesarias con:

```bash
pip install -r requirements.txt

```
Cómo ejecutar el pipeline

Coloca los archivos prints.json, taps.json y pays.csv en la carpeta data/.


Ejecuta el script principal:
```bash
python src/feature_builder_polars.py
```
El dataset final se guardará automáticamente como archivo .parquet comprimido en:
```bash
output/final_dataset.parquet
