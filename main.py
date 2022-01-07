
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery

#

if __name__ == '__main__':
    pipeline_options = PipelineOptions(
        temp_location='gs://york_temp_files/tmp',
        staging_location='gs://york_temp_files/staging',
        region='us-central1',
        runner='DataflowRunner',
        job_name='donald-bledsoe-final-job',
        project='york-cdf-start',
        dataset='final_input_data'
    )

    # Schema definition for product views output table
    views_schema = {
        'fields': [
            {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'total_no_of_product_views', 'type': 'INTEGER', 'mode': 'REQUIRED'}

        ]
    }

    # Schema definition for sales amount output table
    sales_schema = {
        'fields': [
            {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'total_sales_amount', 'type': 'FLOAT', 'mode': 'REQUIRED'}

        ]
    }

    # Table specification for the BQ tables
    views_spec = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='final_donald_bledsoe',
        tableId='cust_tier_code-sku-total_no_of_product_views'
    )

    sales_spec = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='final_donald_bledsoe',
        tableId='cust_tier_code-sku-total_sales_amount'
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:           # Join of customers and product views, and customers and orders tables
        data3 = (
                pipeline | 'Query tables' >> beam.io.ReadFromBigQuery(
                    query='SELECT table2.cust_tier_code, CAST(table1.sku as int) as sku, COUNT(table1.sku) as total_no_of_product_views FROM `york-cdf-start.final_input_data.product_views` as table1 '
                        'JOIN `york-cdf-start.final_input_data.customers` as table2 ON table1.customer_id = table2.customer_id GROUP BY table2.cust_tier_code, table1.SKU ORDER BY table1.sku, table2.CUST_TIER_CODE asc',
                            project="york-cdf-start", use_standard_sql=True)
        )

        data4 = (
                pipeline | 'Query tables2' >> beam.io.ReadFromBigQuery(
                    query='SELECT table2.cust_tier_code, table1.sku, ROUND (SUM (table1.order_amt), 2) as total_sales_amount FROM `york-cdf-start.final_input_data.orders`as table1 JOIN `york-cdf-start.final_input_data.customers` as table2 '
                        'ON table1.customer_id = table2.customer_id GROUP BY table2.cust_tier_code, table1.SKU ORDER BY table1.sku, table2.CUST_TIER_CODE asc', project="york-cdf-start", use_standard_sql=True)
            )

        # Outputting the two tables to BigQuery.
        data3 | "Write1" >> beam.io.WriteToBigQuery(
            views_spec,
            schema=views_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,

        )

        data4 | "Write2" >> beam.io.WriteToBigQuery(
            sales_spec,
            schema=sales_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,

        )
