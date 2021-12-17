# Load csv data from cloud storage  into BigQuery

### Step1 : Go to GCP Console 

- Make two bucket "staging-data-scraper" and processed-data from apache-beam"


- Make dataset " jobs-info-naukri" : Create two database "jobs-table-with-date-time-and-url"

  and Master Database "master-table"

  Google Data Studio connect with Master Database

- make cloud function "load-csv"

  (BigQuery  Documentation Loading CSV data from cloud  storage)

  [https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv]()

### Step2: In cloud function:

make a file "single-csv-to-bq" and "multiple-csv-to-bq"

```python
import logging
from google.cloud import bigquery, client
import datetime
from my_data import data, mail_addr, table
from sending_mail import send_email
def csv_loader(event,context):
    client = bigquery.Client()

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
        #send_email(["email_id"],'successfull loaded the file '+str(uri)+' to bigquery')
        sub = ' file ' + str(uri) + ' loaded successfully ' + ' into bigquery at ' + str(x)
        with open('success.txt') as myfile:
            success_msg= myfile.read()
        send_email(mail_addr,sub,success_msg)


    except Exception as e:
        logging.error(e)
    #send_email(["email_id"],'')
        x=datetime.datetime.now().replace(microsecond=0)
        sub=' File ' +str(uri) +' is not loaded into bigquery at '+ str(x) + '. error found : '+ str(e)
        with open('failure.txt') as f:
            failed_msg= f.read()
        send_email(mail_addr, sub,failed_msg)
        logging.info('Error Occured while loading data')
```

Process flow:

![process_flow_by_prateek](C:\Users\prateek\OneDrive\Documents\process_flow_by_prateek.png)
