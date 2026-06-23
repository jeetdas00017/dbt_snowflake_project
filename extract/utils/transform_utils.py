import pandas as pd

from extract.utils.logging_config import logger


def normalize_timestamp_columns(df: pd.DataFrame) -> pd.DataFrame:
    timestamp_cols = [
        col for col in df.columns
        if col.lower().endswith(("_at", "_date"))
    ]

    if not timestamp_cols:
        logger.debug("No timestamp columns to normalize")
        return df

    logger.info("Normalizing timestamp columns for parquet: %s", timestamp_cols)

    for col in timestamp_cols:
        if col not in df.columns:
            continue

        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            parsed = pd.to_datetime(df[col], errors="coerce", utc=False)
            if parsed.isna().all() and df[col].notna().any():
                logger.warning(
                    "Timestamp parse produced all nulls for %s; original dtype=%s",
                    col,
                    df[col].dtype,
                )
            elif parsed.isna().sum() > 0:
                logger.warning(
                    "Timestamp parse produced %s nulls for %s",
                    parsed.isna().sum(),
                    col,
                )
            df[col] = parsed

        if pd.api.types.is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)

        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                df[col] = df[col].astype("datetime64[us]")
            except Exception as exc:
                logger.warning(
                    "Failed to cast column %s to datetime64[us]: %s",
                    col,
                    exc,
                )
        else:
            try:
                df[col] = df[col].astype("datetime64[us]")
            except Exception as exc:
                logger.warning(
                    "Failed to downcast column %s to datetime64[us]: %s",
                    col,
                    exc,
                )

    return df
