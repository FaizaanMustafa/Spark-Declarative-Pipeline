from pyspark import pipelines as dp

dp.create_streaming_table(
    name = 'dim_products'
)

dp.create_auto_cdc_flow(
    target='dim_products',
    source='products_enr_view',
    keys= ['product_id'],
    sequence_by='last_updated',
    stored_as_scd_type=2
)