from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

@dp.table(
    name = 'customers_stg'
)
def customers_stg():
    df = spark.readStream.table('sdp_cata.sdp_schema.customers')
    return df