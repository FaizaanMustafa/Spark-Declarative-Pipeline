from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp


my_rules_one = {
    "rule1":"sales_id is not null"
}


dp.create_streaming_table(
    name = 'sales_stg',
    expect_all_or_drop=my_rules_one
)


@dp.append_flow(
    target = 'sales_stg'
)
def east_sales():
    df = spark.readStream.table('sdp_cata.sdp_schema.sales_east')
    return df


@dp.append_flow(
    target = 'sales_stg'
)
def west_sales():
    df = spark.readStream.table('sdp_cata.sdp_schema.sales_west')
    return df
