# Data movement from Google Cloud storage to BigQuery through Cloud Functions

## Overview Of Cloud Functions

Cloud Functions is a lightweight compute solution for developers to create single purpose, stand-alone functions that respond to Cloud events without the need to manage a server or runtime environment.



###  Create Cloud Functions

Open the Functions Overview page in the Cloud Console:Go to the Cloud Functions Overview page Make sure that the project for which you enabled Cloud Functions is selected.

Click Create function.

Name your function “load-csv”.

In the Trigger field, select Cloud Storage .

In the Authentication field, select Allow unauthenticated invocations.

Click Save to save your changes, and then click Next.In the Source code field, select Inline editor. 

In this exercise, you will use the default function provided in the editor.

Use the Runtime dropdown to select the desired Python runtime.

```
https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
```

```
https://cloud.google.com/functions/docs/writing#types_of_cloud_functions
```

```
https://cloud.google.com/functions/docs/calling/storage
```



## Design

![design_gcs_to_BQ](C:\Users\prateek\OneDrive\Desktop\khatauni\design_gcs_to_BQ.png)



