import polars as pl
from data_loader_polars import load_all_data
from pathlib import Path

# ====================
# UTILS
# ====================

def build_join_key(df: pl.DataFrame) -> pl.DataFrame:
    """
    Crea una columna 'join_key' combinando día, user_id y value_prop
    para usarla como clave única entre prints y taps.
    """
    return df.with_columns([
        (pl.col("day").cast(pl.Utf8) + "_" +
         pl.col("user_id").cast(pl.Utf8) + "_" +
         pl.col("value_prop")).alias("join_key")
    ])

# ====================
# PASO 1: Filtrar prints de la última semana
# ====================

def get_last_week_prints(prints: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """
    Devuelve los prints de la última semana disponible en el dataset.
    """
    latest_week = prints["week"].max()
    filtered = prints.filter(pl.col("week") == latest_week)
    return filtered, latest_week

# ====================
# PASO 2: Detectar si hubo click (tap)
# ====================

def mark_clicks(last_week_prints: pl.DataFrame, taps: pl.DataFrame) -> pl.DataFrame:
    """
    Marca si cada print de la última semana fue clickeado (tap).
    """
    taps_flagged = build_join_key(taps).with_columns([
        pl.lit(1).alias("clicked")
    ]).select(["join_key", "clicked"]).unique()

    last_week_prints = build_join_key(last_week_prints)

    return (
        last_week_prints
        .join(taps_flagged, on="join_key", how="left")
        .with_columns(pl.col("clicked").fill_null(0).cast(pl.Int8))
        .drop("join_key")
    )

# ====================
# PASO 3: Agregaciones históricas
# ====================

def get_historical_aggregates(prints, taps, pays, weeks_prev):
    """
    Calcula las agregaciones para las 3 semanas anteriores:
    - cantidad de prints
    - cantidad de taps
    - cantidad e importe de pagos
    """
    agg_prints = (
        prints.filter(pl.col("week").is_in(weeks_prev))
        .group_by(["user_id", "value_prop"])
        .agg(pl.count().alias("n_prints_prev3w"))
    )

    agg_taps = (
        taps.filter(pl.col("week").is_in(weeks_prev))
        .group_by(["user_id", "value_prop"])
        .agg(pl.count().alias("n_taps_prev3w"))
    )

    agg_pays = (
        pays.filter(pl.col("week").is_in(weeks_prev))
        .group_by(["user_id", "value_prop"])
        .agg([
            pl.count().alias("n_pays_prev3w"),
            pl.sum("total").alias("sum_pays_prev3w")
        ])
    )

    return agg_prints, agg_taps, agg_pays

# ====================
# PASO 4: Construcción del dataset final
# ====================

def build_training_dataset(prints, taps, pays) -> pl.DataFrame:
    """
    Construye el dataset final combinando:
    - prints de la última semana
    - clics realizados
    - agregaciones históricas de 3 semanas
    """
    last_week_prints, latest_week = get_last_week_prints(prints)
    last_week_prints = mark_clicks(last_week_prints, taps)

    weeks_prev = [latest_week - 3, latest_week - 2, latest_week - 1]
    agg_prints, agg_taps, agg_pays = get_historical_aggregates(prints, taps, pays, weeks_prev)

    df = last_week_prints.join(agg_prints, on=["user_id", "value_prop"], how="left")
    df = df.join(agg_taps, on=["user_id", "value_prop"], how="left")
    df = df.join(agg_pays, on=["user_id", "value_prop"], how="left")

    return df.fill_null(0)

# ====================
# MAIN
# ====================

def main():
    """
    Ejecuta el pipeline completo y guarda el dataset como parquet.
    """
    # Cargar datos
    prints, taps, pays = load_all_data()

    # Construir dataset
    df = build_training_dataset(prints, taps, pays)

    # Guardar como Parquet
    output_path = Path("output/final_dataset.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)

    print(f"✅ Dataset final guardado en: {output_path}")

if __name__ == "__main__":
    main()
