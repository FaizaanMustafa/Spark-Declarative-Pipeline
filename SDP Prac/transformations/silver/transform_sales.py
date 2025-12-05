from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp


@dp.view(
    name = 'sales_enr_view'
)
def sales_stg_trns():
    df = spark.readStream.table('sales_stg')
    df = df.withColumn("total_amount",col("quantity") * col("amount"))
    return df


dp.create_streaming_table(
    name = 'sales_enr'
)

dp.create_auto_cdc_flow(
    target="sales_enr",
    source="sales_enr_view",
    keys = ["sales_id"],
    sequence_by="sale_timestamp",
    stored_as_scd_type=1
    )