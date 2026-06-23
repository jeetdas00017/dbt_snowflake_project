import pyarrow as pa
import pyarrow.parquet as pq

from extract.utils.config import PARQUET_COMPRESSION, PARQUET_TIMESTAMP_UNIT
from extract.utils.logging_config import logger


def write_dataframe_to_parquet(df, local_path: str) -> None:
    logger.info("Writing DataFrame to parquet: %s", local_path)
    table = pa.Table.from_pandas(df)
    pq.write_table(
        table,
        local_path,
        compression=PARQUET_COMPRESSION,
        coerce_timestamps=PARQUET_TIMESTAMP_UNIT,
        allow_truncated_timestamps=True,
        use_deprecated_int96_timestamps=False,
    )
