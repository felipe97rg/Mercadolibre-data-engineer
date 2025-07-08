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

## 📂 Estructura del proyecto

MercadoPago/
├── src/
│ ├── data_loader_polars.py # Módulo para cargar y limpiar datos
│ ├── feature_builder_polars.py # Pipeline completo de construcción del dataset
├── data/ # Archivos de entrada (prints.json, taps.json, pays.csv)
├── output/
│ └── final_dataset.parquet # Dataset final generado
├── requirements.txt # Dependencias
└── README.md # Este archivo


---

## ⚙️ Requisitos

Python ≥ 3.9

Instalar las dependencias necesarias con:

```bash
pip install -r requirements.txt


Cómo ejecutar el pipeline

Coloca los archivos prints.json, taps.json y pays.csv en la carpeta data/.

Ejecuta el script principal:

python src/feature_builder_polars.py

El dataset final se guardará automáticamente como archivo .parquet comprimido en:

output/final_dataset.parquet
