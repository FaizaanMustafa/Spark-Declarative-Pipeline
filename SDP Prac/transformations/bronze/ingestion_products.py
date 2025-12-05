from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

@dp.table(
    name = 'products_stg'
)
def products_stg():
    df = spark.readStream.table('sdp_cata.sdp_schema.products')
    return df