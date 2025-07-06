import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")


def expand_event_data(df):
    """Expande la columna event_data si existe"""
    if "event_data" in df.columns:
        event_df = df["event_data"].apply(pd.Series)
        df = pd.concat([df.drop(columns=["event_data"]), event_df], axis=1)
    return df


def add_temporal_features(df):
    """
    Detecta automáticamente la columna de fecha entre ['pay_date', 'day'],
    la convierte a datetime y crea columnas auxiliares 'timestamp', 'day' y 'week'.
    """
    posibles_fechas = ["pay_date", "day"]
    fecha_col = None

    # Detectar columna de fecha existente
    for col in posibles_fechas:
        if col in df.columns:
            fecha_col = col
            break

    if not fecha_col:
        raise ValueError("No se encontró ninguna columna de fecha en el DataFrame.")

    # Convertir a datetime y crear columnas estándar
    df[fecha_col] = pd.to_datetime(df[fecha_col])
    df["timestamp"] = df[fecha_col]  # referencia interna
    df["day"] = df["timestamp"].dt.date
    df["week"] = pd.to_datetime(df["day"]).dt.isocalendar().week

    return df


def clean_df(df, name=""):
    """Elimina duplicados y filas con nulos"""
    before = df.shape[0]
    df = df.drop_duplicates()
    df = df.dropna()
    after = df.shape[0]
    print(f"{name}: Filas limpias -> {after} (eliminadas {before - after})")
    return df


def load_prints():
    path = DATA_DIR / "prints.json"
    df = pd.read_json(path, lines=True)
    df = expand_event_data(df)
    df = add_temporal_features(df)
    df = clean_df(df, "PRINTS")
    return df


def load_taps():
    path = DATA_DIR / "taps.json"
    df = pd.read_json(path, lines=True)
    df = expand_event_data(df)
    df = add_temporal_features(df)
    df = clean_df(df, "TAPS")
    return df


def load_pays():
    path = DATA_DIR / "pays.csv"
    df = pd.read_csv(path)
    df = add_temporal_features(df)
    df = clean_df(df, "PAYS")
    return df



def load_all_data():
    prints = load_prints()
    taps = load_taps()
    pays = load_pays()
    return prints, taps, pays
