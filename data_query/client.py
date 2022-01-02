from pyathena import connect

from data_query.settings import (
    ATHENA_USERNAME,
    ATHENA_PASSWORD,
    AWS_REGION,
    S3_LOCATION,
)

from pandas import read_sql_query, DataFrame


class AthenaClient:
    def __init__(self) -> None:
        self._conn = connect(
            s3_staging_dir=S3_LOCATION,
            region_name=AWS_REGION,
            aws_access_key_id=ATHENA_USERNAME,
            aws_secret_access_key=ATHENA_PASSWORD,
        )

    def get_query_result(self, query: str, n: int) -> DataFrame:
        df = read_sql_query(query, self._conn)

        return df.head(n=n)
