from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp


@dp.view(
    name = 'products_enr_view'
)
def products_enr_view():
    df = spark.readStream.table('products_stg')
    return df


dp.create_streaming_table(
    name = 'products_enr'
)

dp.create_auto_cdc_flow(
    target='products_enr',
    source='products_enr_view',
    keys= ['product_id'],
    sequence_by= 'last_updated',
    stored_as_scd_type=1
)