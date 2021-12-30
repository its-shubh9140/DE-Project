##        Apache Beam-Cleaning and Transformation

### Overview:

[Apache Beam](http://beam.apache.org/)  is an open source, unified model for defining both batch- and streaming-data parallel-processing pipelines. The Apache Beam programming model simplifies the mechanics of large-scale data processing. The project is divided into two parts: the programming model SDKs and the model implementations, also known as “runners''([Google Cloud Dataflow](http://cloud.google.com/dataflow/)). A programmer will describe a processing pipeline using one of the Beam SDKs, which is then run on one of the Beam runners, which will actually execute the pipeline.  

### Data Cleaning and Transformation:

######                            <img src="C:\Users\91832\Downloads\data pipe.PNG" alt="data pipe" style="zoom:50%;" />

​										Fig. Data Cleaning and Transformation through Apache Beam.

####    Steps:

1. Pcollection-Reads data from scraped data.

2. Ptransform- Apply transformation to clean data. Here basically we are applying a cleaning transform for removing unwanted commas,slashes, single and double quotes, spaces , missing values, angle brackets,square brackets etc. Also splitting data points, namely location for different job roles. 

3. Write to GCS-In final step we run dataflow template to run our data pipeline and generated output will store in google cloud storage bucket.

   #### Workflow of pipeline.py

   The workflow of pipeline.py is given below:

   1. Importing required modules

   2. Setting Google Environment Path

   3. Creating transformation functions

   4. Adding system arguments

   5. Writing the pipeline code

   6. Creating PCollection and PTransforms

   7. Running the Pipeline

   ### Install

   ```
   pip install -r requirements.txt
   ```

   ### Run locally using Direct Runner

   ```
   python main.py \
   --input data<path-of-input_file>.csv \
   --output data<path-of-output-file>.csv
   ```

#### Run on Dataflow Runner

```
--input gs://staging-data-scraper/jobs_with_date_time.csv \
--output gs://process-data-from-apache_beam/output_1 \
--runner DataflowRunner\
--project de-training-project\
--staging_location gs://process-data/staging\
--template_location gs://process-data/templates/naukari_template\
--region us-central1
```

### Steps to run template on Dataflow

#### set up your Environment:

Enable <b>Dataflow API</b> and set your <b> GOOGLE_APPLICATION_CREDENTIALS</b> .

      1. Go to Dataflow on google cloud platform.
      2. select create job from template.
      3. Provide job name .
      4. From Dataflow template select custom template option.
      5. In custom template , select template from bucket .
      6. provide temporary location.
      7. Click on Run Job.

