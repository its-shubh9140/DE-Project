import logging
import re
import os
import csv
import apache_beam as beam
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:\\Users\\91832\\Desktop\\training\\de-training-project-be3fd58db39d.json'


# defining custom arguments
class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input')
        parser.add_value_provider_argument('--output')


class SplitCSV(beam.DoFn):

    def process(self, line, *args, **kwargs):

        row = list(csv.reader([line]))[0]
        roles = row[0].replace(',', '').replace('"', '')
        company = row[1].replace(',', '').replace('"', '')


        # formating locations string
        locs = row[2]
        locs = re.sub(r"\(.*\)+", "", locs) \
            .replace(',/,', '/') \
            .replace(',anywhere,in,india', '') \
            .replace(r'\n', '') \
            .replace("'", '') \
            .replace('"', "") \
            .replace(",", ' ')

        locs = re.sub(r"\s+", ",", locs)  # replacing multiple spaces with ,

        experience = row[3].replace(',', '').replace('"', '')

        skills = row[4][1:-1].replace("'", '').replace('"', '')  # replacing ' with ,

        after_skill = row[5:]

        for loc in list(locs[1:-1].split(',')):  # spliting locations

            if loc != '':  # only if loc is not None
                if '/' in loc:  # if / in loc then take 1st value else loc
                    if loc == '/':  # if loc == / don't do anything
                        continue
                    yield '"'+roles+'","' +company.strip(',') + '","' + loc.split('/')[0].strip(
                        ' ').capitalize() + '","' + experience + '","' + skills + '",' + ','.join(after_skill)+',"'+str(datetime.now())+'"'
                else:
                    yield '"'+roles+'"," ' +company.strip(',') + '","' + loc.strip(
                        ' ').capitalize() + '","' + experience + '","' + skills + '",' + ','.join(after_skill)+',"'+str(datetime.now())+'"'


def run(argv=None):
    # instantiate the pipeline
    pipeline_options = PipelineOptions()
    known_args = pipeline_options.view_as(UserOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as pipeline:
        job = (
                pipeline
                | 'ReadFile' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
        )

        job_1 = job | 'SplitData' >> beam.ParDo(SplitCSV())

        (
                job_1 | "WriteToGCS" >> beam.io.WriteToText(
            file_path_prefix=known_args.output,
            file_name_suffix='.csv',
            header='roles,companies,locations,experience,skills,job_posted_date,scraper_run_date_time,jd_url,inserted_datetime'
        )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

