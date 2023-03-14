import argparse
import logging
import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import os

# ParDo Class for Parallel processing by applying user defined transformations

class convert_row(beam.DoFn):
    def process(self,element):
        try:
            line = element.split(',')
            tp =line[0] + ',' + line[1] + ',' + line[2] + ',' + line[3]
            tp = tp.replace('"','')
            tp = tp.split()
            return tp
        except:
            logging.info('Some error occured')

# Entry run method for triggering pipeline
    def run(self):
        parser =argparse.ArgumentParser()
        parser.add_argument('--input',
                            dest='input',
                            default='gs://gcp-demobucket',
                            help='Input file to process.')
        parser.add_argument('--output',
                            dest='output',
                            required=True,
                            help='Output file to write results to.')
        known_args, pipeline_args = parser.parse_known_args()

        qry = ''' select Symbol, Sum(QuantityTraded) as QuantityTraded 
                  from (
                          select Symbol,
                                 case when BuyorSell ='BUY' then cast(replace(QuantityTraded,',','') as int64)
                                 else -cast(replace(QuantityTraded,',','') as int64) end as QuantityTraded
                  from PCOLLECTION
                        )
                  group by 1'''

        #Main Pipeline

        with beam.pipeline(options = PipelineOptions(pipeline_args)) as p:
            lines = p | 'read' >> ReadFromText(known_args.input,skip_header_lines=1)
            counts = (
                        lines
                        | 'Formatted rows of strings' >> beam.ParDo(convert_row())
                        | 'Convert as table rows' >> beam.map(lambda x:beam.Row(Symbol = str(x.split(',')[0]),BuyorSell = str(x.split(',')[1]), QuantityTraded = str(x.split(',')[2])))
                        | 'Convert to Bigquery readable Dict' >> beam.Map(lambda row : row._asdict())
            )

        # Write to BigQuery
        counts| 'Write to BigQuery' >> beam.io.Write(
                                                    beam.io.WriteToBigQuery(
                                                                                'batch_data',
                                                                                dataset= 'dataflow_demo',
                                                                                project='gcpproject-380209',
                                                                                schema= 'Symbol:STRING,QuantityTraded:INTEGER',
                                                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                                write_disposition= beam.io.BigQueryDisposition.WRITE_TRUNCATE
                                                                           )
                                                    )

        # Trigger entry function here
        if __name__ == '__main__':
            logging.getLogger().setLevel(logging.INFO)
            self.run()
