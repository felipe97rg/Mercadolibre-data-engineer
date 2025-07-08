import polars as pl
from polars import DataType
from pathlib import Path

DATA_DIR = Path("data")

def expand_event_data(df: pl.DataFrame) -> pl.DataFrame:

    """Expande la columna event_data si existe y es tipo Struct"""
    # Verificar si la columna 'event_data' existe y es de tipo Struct
    if "event_data" in df.columns and df.schema["event_data"] == pl.Struct:
        # Extraer la columna struct y expandirla
        expanded = df.select("event_data").unnest("event_data")
        df = df.drop("event_data").hstack(expanded)
    return df


def add_temporal_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Asegura que exista una columna 'day' en formato fecha y
    genera la columna auxiliar 'week' correspondiente.
    Si 'pay_date' existe, la renombra a 'day' para unificar el tratamiento.
    """
    # Renombrar 'pay_date' a 'day' si existe
    if "pay_date" in df.columns and "day" not in df.columns:
        df = df.rename({"pay_date": "day"})

    # Verificar que existe 'day'
    if "day" not in df.columns:
        raise ValueError("No se encontró ninguna columna de fecha válida (ni 'day' ni 'pay_date').")

    # Detectar tipo de columna
    tipo_col = df.schema["day"]

    # Si es string, convertirla a tipo fecha
    if tipo_col == pl.Utf8:
        df = df.with_columns([
            pl.col("day").str.to_date(strict=False)
        ])

    # Generar columna de semana
    df = df.with_columns([
        pl.col("day").dt.week().alias("week")
    ])

    return df



def clean_df(df: pl.DataFrame, name="") -> pl.DataFrame:

    """Elimina duplicados y nulos"""

    before = df.shape[0]
    df = df.unique().drop_nulls()
    after = df.shape[0]
    print(f"{name}: Filas limpias -> {after} (eliminadas {before - after})")
    return df


def load_prints():

    """Carga el dataset de prints y lo limpia"""

    path = DATA_DIR / "prints.json"
    df = pl.read_ndjson(path)
    df = expand_event_data(df)
    df = add_temporal_features(df)
    df = clean_df(df, "PRINTS")
    return df


def load_taps():

    """Carga el dataset de taps y lo limpia"""

    path = DATA_DIR / "taps.json"
    df = pl.read_ndjson(path)
    df = expand_event_data(df)
    df = add_temporal_features(df)
    df = clean_df(df, "TAPS")
    return df


def load_pays():

    """Carga el dataset de pays y lo limpia"""

    path = DATA_DIR / "pays.csv"
    df = pl.read_csv(path)
    df = add_temporal_features(df)
    df = clean_df(df, "PAYS")
    return df


def load_all_data():

    """Carga todos los datasets y los devuelve como tupla"""

    prints = load_prints()
    taps = load_taps()
    pays = load_pays()
    return prints, taps, pays