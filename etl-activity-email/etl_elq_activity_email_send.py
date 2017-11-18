"""
    Beam/Dataflow pipeline to export Eloqua email send activities and load
    in to BigQuery
"""

import apache_beam as beam

from etl_elq_activities import export_elq_activity

# Constants

PROJECT = 'gcp-project-name'
DISPOSITION = beam.io.gcp.bigquery.BigQueryDisposition.WRITE_APPEND

def run():
    """ Define and run Apache Beam pipeline """

    argv = [
        '--project={0}'.format(PROJECT),
        '--job_name=etleloquaactivityemailsend',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(PROJECT),
        '--temp_location=gs://{0}/staging/'.format(PROJECT),
        '--runner=DataflowRunner',
        '--setup_file=./setup.py'
    ]

    pipeline = beam.Pipeline(argv=argv)

    activities = pipeline | 'GetEloquaActivityEmailSend' >> beam.Create(export_elq_activity(
        'EmailSend', '2017-10-01 00:00:00', '2017-10-02 00:00:00'
        ))

    write = activities | 'WriteToBQ' >> beam.io.WriteToBigQuery(
        table='activity_email_send',
        dataset='eloqua',
        project=PROJECT,
        write_disposition=DISPOSITION
    )

    pipeline.run()


if __name__ == '__main__':
    run()
