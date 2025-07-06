# generate_training_data.py

import polars as pl
from pathlib import Path
from data_loader_polars import load_all_data


def get_last_week(df: pl.DataFrame) -> int:
    """Devuelve el número de semana más reciente en el DataFrame."""
    return df.select(pl.col("week").max()).item()


def label_clicked(prints: pl.DataFrame, taps: pl.DataFrame) -> pl.DataFrame:
    """
    Une prints con taps para generar la columna 'clicked'.
    Devuelve 1 si hubo clic en esa value_prop por ese usuario el mismo día.
    """
    taps_subset = taps.select(["user_id", "value_prop", "day"]).with_columns([
        pl.lit(1).alias("clicked")
    ])

    return prints.join(taps_subset, on=["user_id", "value_prop", "day"], how="left").with_columns(
        pl.col("clicked").fill_null(0)
    )


def add_3week_aggregations(
    prints: pl.DataFrame, 
    taps: pl.DataFrame, 
    pays: pl.DataFrame
) -> pl.DataFrame:
    """
    Para cada fila del DataFrame de prints de la última semana,
    calcula las estadísticas agregadas de las 3 semanas anteriores.
    """
    result = []

    for row in prints.iter_rows(named=True):
        user = row["user_id"]
        vp = row["value_prop"]
        day = row["day"]
        date_start = day - pl.duration(days=21)
        date_end = day - pl.duration(days=1)

        # FILTROS
        p3 = prints.filter(
            (pl.col("user_id") == user) &
            (pl.col("value_prop") == vp) &
            (pl.col("day") >= date_start) & (pl.col("day") <= date_end)
        ).shape[0]

        t3 = taps.filter(
            (pl.col("user_id") == user) &
            (pl.col("value_prop") == vp) &
            (pl.col("day") >= date_start) & (pl.col("day") <= date_end)
        ).shape[0]

        pays_filtered = pays.filter(
            (pl.col("user_id") == user) &
            (pl.col("value_prop") == vp) &
            (pl.col("day") >= date_start) & (pl.col("day") <= date_end)
        )

        pay_count = pays_filtered.shape[0]
        total_amount = pays_filtered["total"].sum() if pay_count > 0 else 0

        result.append({
            "user_id": user,
            "value_prop": vp,
            "day": day,
            "prints_prev3w": p3,
            "taps_prev3w": t3,
            "pays_prev3w": pay_count,
            "total_amount_prev3w": total_amount,
        })

    agg_df = pl.DataFrame(result)
    return prints.join(agg_df, on=["user_id", "value_prop", "day"], how="left")


if __name__ == "__main__":
    prints, taps, pays = load_all_data()

    last_week = get_last_week(prints)
    prints_last_week = prints.filter(pl.col("week") == last_week)

    prints_labeled = label_clicked(prints_last_week, taps)
    enriched = add_3week_aggregations(prints_labeled, taps, pays)

    output_path = Path("output/final_dataset.parquet")
    enriched.write_parquet(output_path)
    print(f"✅ Dataset generado en: {output_path}")