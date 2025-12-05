from pyspark import pipelines as dp

dp.create_streaming_table(
    name = 'dim_customers'
)

dp.create_auto_cdc_flow(
    target='dim_customers',
    source='customers_enr_view',
    keys= ['customer_id'],
    sequence_by='last_updated',
    stored_as_scd_type=2
)