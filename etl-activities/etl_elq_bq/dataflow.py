"""
    Apache Beam / Dataflow pipeline for exporting Eloqua activities and
    loading to BigQuery
"""

import apache_beam as beam
from elq_export import export_elq_activity

# Constants

PROJECT = 'gcp-project-name'

DISPOSITION = beam.io.gcp.bigquery.BigQueryDisposition.WRITE_APPEND

# Mapping of Activity Types to BigQuery table names
TABLE_MAP = {
    'EmailSend': 'activity_email_send',
    'EmailOpen': 'activity_email_open',
    'EmailClickthrough': 'activity_email_click',
    'Bounceback': 'activity_email_hardbounce',
    'Unsubscribe': 'activity_email_unsub'
}

# Eloqua credentials
ELQ_AUTH = {
    'eloqua_company': 'company',
    'eloqua_username': 'username',
    'eloqua_password': 'password'
}

# Functions


def run(activity, start, end):
    """
    Define and run Apache Beam pipeline

    :param str activity: Activity type to export
    :param str start: beginning of export range; format %Y-%m-%d %H:%M:%S
    :param string end: end of export range; format %Y-%m-%d %H:%M:%S
    """

    activity_table = TABLE_MAP[activity]

    job_name = '{activity}{start}'.format(
        activity=activity.lower(),
        start=start.replace('-', '').replace(' ', '').replace(':', '')
    )

    argv = [
        '--project={0}'.format(PROJECT),
        '--job_name=etlactivity{0}'.format(job_name),
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(PROJECT),
        '--temp_location=gs://{0}/staging/'.format(PROJECT),
        '--runner=DataflowRunner',
        '--setup_file=./setup.py'
    ]

    pipeline = beam.Pipeline(argv=argv)

    activities = pipeline | 'GetEloquaActivity{activity}'.format(activity=activity) >> beam.Create(export_elq_activity(
        ELQ_AUTH, activity, start, end
    ))

    write = activities | 'WriteToBQ' >> beam.io.WriteToBigQuery(
        table=activity_table,
        dataset='eloqua',
        project=PROJECT,
        write_disposition=DISPOSITION
    )

    pipeline.run()
