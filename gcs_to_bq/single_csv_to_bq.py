import logging
from google.cloud import bigquery, client
import datetime
from my_data import data, mail_addr, table
from sending_mail import send_email
def csv_loader(event):
    client = bigquery.Client.from_service_account_json("C:\\Users\\prateek\\Downloads\\de-training-project-3fb8c9f7d834.json")

# TODO(developer): Set table_id to the ID of the table to create.
# table_id = "your-project.your_dataset.your_table_name"

    job_config = bigquery.LoadJobConfig(
    schema=[
        #bigquery.SchemaField("serial_no", "INTEGER"),
        bigquery.SchemaField("roles", "STRING"),
        bigquery.SchemaField("companies", "STRING"),
        bigquery.SchemaField("locations", "STRING"),
        bigquery.SchemaField("experience", "STRING"),
        bigquery.SchemaField("skills", "STRING"),
        bigquery.SchemaField("job_posted_date", "DATE"),
        bigquery.SchemaField("scraper_run_date_time", "DATETIME"),
        bigquery.SchemaField("url", "STRING"),

    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
# get this from event
    #uri = "gs://training-demo-project/naukri2.csv"
    uri="gs://"+event['bucket']+"/"+event['name']
    logging.debug(uri)

# static load it from later
    try:
        #table_id="de-training-project.jobs_info_naukri.jobs"
        table_id=table
        logging.debug(table_id)


        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)  # Make an API request.


#mport dat time and str(datetime.now())

        load_job.result()  # Waits for the job to complete.
        destination_table = client.get_table(table_id)  # Make an API request.
        print("Loaded {} rows.".format(destination_table.num_rows))
        x=datetime.datetime.now().replace(microsecond=0)
        #print(x+"Ptr")
        #send_email(["sarojprateekkumar@gmail.com","megha.vishwase@mediaagility.com ","mayuresh.bharmoria@mediaagility.com"],'successfull loaded the file '+str(uri)+' to bigquery')
        sub = ' file ' + str(uri) + ' loaded successfully ' + ' into bigquery at ' + str(x)
        with open('success.txt') as myfile:
            success_msg= myfile.read()
        send_email(mail_addr,sub,success_msg)


    except Exception as e:
        logging.error(e)
    #send_email(["sarojprateekkumar@gmail.com","megha.vishwase@mediaagility.com ","mayuresh.bharmoria@mediaagility.com"],'')
        x=datetime.datetime.now().replace(microsecond=0)
        sub=' File ' +str(uri) +' is not loaded into bigquery at '+ str(x) + '. error found : '+ str(e)
        with open('failure.txt') as f:
            failed_msg= f.read()
        send_email(mail_addr, sub,failed_msg)
        logging.info('Error Occured while loading data')

event={'bucket': 'training-demo-project', 'contentType': 'application/vnd.ms-excel', 'crc32c': 'GgOaXQ==',
        'etag': 'CO7V0o3R2PQCEAE=', 'generation': '1639118730996462',
        'id': 'training-demo-project/csv_file/jobs_with_date_time.csv/1639118730996462', 'kind':
            'storage#object', 'md5Hash': 'a2GXYiRza1n5jWhML9KHYw==', 'mediaLink':
            'https://www.googleapis.com/download/storage/v1/b/training-demo-project/o/csv_file%2Fjobs_with_date_time.csv?generation=1639118730996462&alt=media',
        'metageneration': '1', 'name': 'csv_file/jobs_with_date_time.csv', 'selfLink': 'https://www.googleapis.com/storage/v1/b/training-demo-project/o/csv_file%2Fjobs_with_date_time.csv',
        'size': '797180', 'storageClass': 'STANDARD', 'timeCreated': '2021-12-10T06:45:31.153Z',
        'timeStorageClassUpdated': '2021-12-10T06:45:31.153Z', 'updated': '2021-12-10T06:45:31.153Z'}

csv_loader(event)






