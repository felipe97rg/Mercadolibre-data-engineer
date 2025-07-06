import polars as pl
from polars import DataType
from pathlib import Path

DATA_DIR = Path("data")

def expand_event_data(df: pl.DataFrame) -> pl.DataFrame:
    """Expande la columna event_data si existe y es tipo Struct"""
    if "event_data" in df.columns and df.schema["event_data"] == pl.Struct:
        # Extraer la columna struct y expandirla
        expanded = df.select("event_data").unnest("event_data")
        df = df.drop("event_data").hstack(expanded)
    return df


def add_temporal_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Detecta una columna de fecha y crea columnas 'timestamp', 'day' y 'week'.
    Usa to_date() para strings tipo 'YYYY-MM-DD'.
    """
    posibles_fechas = ["timestamp", "pay_date", "day"]
    fecha_col = next((col for col in posibles_fechas if col in df.columns), None)

    if not fecha_col:
        raise ValueError("No se encontró ninguna columna de fecha válida.")

    tipo_col = df.schema[fecha_col]

    # Si es string, convertir con str.to_date
    if tipo_col == pl.Utf8:
        df = df.with_columns([
            pl.col(fecha_col).str.to_date(strict=False).alias("timestamp")
        ])
    else:
        df = df.with_columns([
            pl.col(fecha_col).alias("timestamp")
        ])

    # Agregar columnas auxiliares
    df = df.with_columns([
        pl.col("timestamp").dt.date().alias("day"),
        pl.col("timestamp").dt.week().alias("week")
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
    path = DATA_DIR / "prints.json"
    df = pl.read_ndjson(path)
    df = expand_event_data(df)
    df = add_temporal_features(df)
    df = clean_df(df, "PRINTS")
    return df


def load_taps():
    path = DATA_DIR / "taps.json"
    df = pl.read_ndjson(path)
    df = expand_event_data(df)
    df = add_temporal_features(df)
    df = clean_df(df, "TAPS")
    return df


def load_pays():
    path = DATA_DIR / "pays.csv"
    df = pl.read_csv(path)
    df = add_temporal_features(df)
    df = clean_df(df, "PAYS")
    return df


def load_all_data():
    prints = load_prints()
    taps = load_taps()
    pays = load_pays()
    return prints, taps, pays
