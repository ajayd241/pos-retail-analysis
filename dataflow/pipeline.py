import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import BigQueryDisposition

from transformations import ParseAndValidate, EnrichTransaction, FormatForBigQuery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# BigQuery schema including derived fields added by EnrichTransaction
BQ_SCHEMA = {
    'fields': [
        {'name': 'transaction_id',        'type': 'STRING',    'mode': 'REQUIRED'},
        {'name': 'store_id',              'type': 'STRING',    'mode': 'REQUIRED'},
        {'name': 'terminal_id',           'type': 'STRING',    'mode': 'REQUIRED'},
        {'name': 'transaction_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'total_amount',          'type': 'FLOAT',     'mode': 'REQUIRED'},
        {'name': 'payment_method',        'type': 'STRING',    'mode': 'NULLABLE'},
        {'name': 'items',                 'type': 'STRING',    'mode': 'NULLABLE'},
        {'name': 'transaction_hour',      'type': 'INTEGER',   'mode': 'NULLABLE'},
        {'name': 'transaction_date',      'type': 'STRING',    'mode': 'NULLABLE'},
        {'name': 'is_high_value',         'type': 'BOOLEAN',   'mode': 'NULLABLE'},
    ]
}

# Dead letter schema for malformed messages
DEAD_LETTER_SCHEMA = {
    'fields': [
        {'name': 'raw_message', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'error',       'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'timestamp',   'type': 'STRING', 'mode': 'NULLABLE'},
    ]
}


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project',          required=True,  help='GCP project ID')
    parser.add_argument('--subscription',     required=True,  help='Pub/Sub subscription path')
    parser.add_argument('--bq_table',         required=True,  help='BigQuery table: project:dataset.table')
    parser.add_argument('--bq_dead_letter',   required=True,  help='BigQuery dead letter table')
    parser.add_argument('--temp_location',    required=True,  help='GCS temp location')
    parser.add_argument('--region',           default='us-central1')
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True

    logger.info(f"Starting pipeline for subscription: {known_args.subscription}")

    with beam.Pipeline(options=options) as p:

        # Step 1 — Read raw messages from Pub/Sub
        raw_messages = (
            p
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(
                subscription=known_args.subscription
            )
        )

        # Step 2 — Parse and validate, splitting into valid and dead letter
        parsed = (
            raw_messages
            | 'Parse and Validate' >> beam.ParDo(ParseAndValidate()).with_outputs(
                ParseAndValidate.DEAD_LETTER,
                main=ParseAndValidate.VALID
            )
        )

        # Step 3 — Enrich valid records with derived fields
        enriched = (
            parsed[ParseAndValidate.VALID]
            | 'Enrich Transactions' >> beam.ParDo(EnrichTransaction())
        )

        # Step 4 — Format for BigQuery schema
        formatted = (
            enriched
            | 'Format for BigQuery' >> beam.ParDo(FormatForBigQuery())
        )

        # Step 5 — Write valid records to BigQuery
        (
            formatted
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                table=known_args.bq_table,
                schema=BQ_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # Step 6 — Write dead letter records to separate table
        (
            parsed[ParseAndValidate.DEAD_LETTER]
            | 'Write Dead Letters' >> beam.io.WriteToBigQuery(
                table=known_args.bq_dead_letter,
                schema=DEAD_LETTER_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == '__main__':
    run()