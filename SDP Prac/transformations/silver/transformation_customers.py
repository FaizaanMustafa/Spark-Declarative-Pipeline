from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp


@dp.view(
    name = 'customers_enr_view'
)
def customers_enr_view():
    df = spark.readStream.table('customers_stg')
    return df


dp.create_streaming_table(
    name = 'customers_enr'
)

dp.create_auto_cdc_flow(
    target="customers_enr",
    source="customers_enr_view",
    keys = ["customer_id"],
    sequence_by="last_updated",
    stored_as_scd_type=1
    )