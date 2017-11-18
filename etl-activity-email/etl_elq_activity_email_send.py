"""
    Beam/Dataflow pipeline to export Eloqua email send activities and load
    in to BigQuery
"""

import apache_beam as beam

from etl_elq_activities import export_elq_activity

# Constants

PROJECT = 'gcp-project-name'


def run():
    """ Define and run Apache Beam pipeline """

    argv = [
        '--project={0}'.format(PROJECT),
        '--job_name=etl_eloqua_activity_email_send',
        '--save_main_session',
        '--runner=DataflowRunner'
    ]

    pipeline = beam.Pipeline(argv=argv)

    (pipeline
     | 'GetEloquaActivityEmailSend' >> beam.Create(export_elq_activity(
         'EmailSend', '2017-10-01 00:00:00', '2017-10-02 00:00:00'
         ))
     | 'WriteToBQ' >> beam.io.gcp.bigquery.BigQuerySink(
         table='activity_email_send',
         dataset='eloqua',
         project=PROJECT
     )
    )

    pipeline.run()


if __name__ == '__main__':
    run()
