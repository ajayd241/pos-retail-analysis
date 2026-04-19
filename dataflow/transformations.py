import json
import logging
from datetime import datetime
import apache_beam as beam

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = [
    'transaction_id',
    'store_id', 
    'terminal_id',
    'transaction_timestamp',
    'total_amount'
]

class ParseAndValidate(beam.DoFn):
    """
    Parses raw Pub/Sub message bytes into a dict.
    Valid records are tagged 'valid'.
    Invalid records are tagged 'dead_letter' for error handling.
    """

    VALID = 'valid'
    DEAD_LETTER = 'dead_letter'

    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        try:
            # Decode bytes to string and parse JSON
            if isinstance(element, bytes):
                element = element.decode('utf-8')
            record = json.loads(element)

            # Validate all required fields are present
            missing = [f for f in REQUIRED_FIELDS if f not in record]
            if missing:
                raise ValueError(f"Missing required fields: {missing}")

            # Validate total_amount is a positive number
            record['total_amount'] = float(record['total_amount'])
            if record['total_amount'] < 0:
                raise ValueError(f"total_amount cannot be negative: {record['total_amount']}")

            # Tag as valid
            yield beam.pvalue.TaggedOutput(self.VALID, record)

        except Exception as e:
            # Tag as dead letter with error context
            yield beam.pvalue.TaggedOutput(self.DEAD_LETTER, {
                'raw_message': str(element),
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            })


class EnrichTransaction(beam.DoFn):
    """
    Adds derived fields to validated transaction records.
    These enrichments make the data more useful for analytics.
    """

    def process(self, element):
        try:
            # Parse timestamp to extract hour for time-based analytics
            ts = datetime.fromisoformat(
                element['transaction_timestamp'].replace('Z', '+00:00')
            )
            element['transaction_hour'] = ts.hour
            element['transaction_date'] = ts.strftime('%Y-%m-%d')

            # Flag high value transactions (over $100)
            element['is_high_value'] = element['total_amount'] > 100.0

            # Normalize payment method to uppercase
            if element.get('payment_method'):
                element['payment_method'] = element['payment_method'].upper()

            # Set default for nullable items field
            if not element.get('items'):
                element['items'] = '[]'

            yield element

        except Exception as e:
            logger.error(f"Enrichment failed for record {element.get('transaction_id')}: {e}")
            yield element


class FormatForBigQuery(beam.DoFn):
    """
    Final formatting pass before writing to BigQuery.
    Ensures all fields match the BigQuery schema exactly.
    """

    def process(self, element):
        yield {
            'transaction_id':        str(element.get('transaction_id', '')),
            'store_id':              str(element.get('store_id', '')),
            'terminal_id':           str(element.get('terminal_id', '')),
            'transaction_timestamp': str(element.get('transaction_timestamp', '')),
            'total_amount':          float(element.get('total_amount', 0.0)),
            'payment_method':        str(element.get('payment_method', '')),
            'items':                 str(element.get('items', '[]')),
            'transaction_hour':      int(element.get('transaction_hour', 0)),
            'transaction_date':      str(element.get('transaction_date', '')),
            'is_high_value':         bool(element.get('is_high_value', False)),
        }