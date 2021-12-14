import json

from google.cloud import storage


def list_blobs_with_prefix(bucket_name, prefix, delimiter=None):
    """Lists all the blobs in the bucket that begin with the prefix.

    This can be used to list all blobs in a "folder", e.g. "public/".

    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:

        a/1.txt
        a/b/2.txt

    If you specify prefix ='a/', without a delimiter, you'll get back:

        a/1.txt
        a/b/2.txt

    However, if you specify prefix='a/' and delimiter='/', you'll get back
    only the file directly under 'a/':

        a/1.txt

    As part of the response, you'll also get back a blobs.prefixes entity
    that lists the "subfolders" under `a/`:

        a/b/
    """

    storage_client = storage.Client.from_service_account_json("C:\\Users\\prateek\\Downloads\\de-training-project-3fb8c9f7d834.json")

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

    #print("Blobs:")
    filelist=[]
    for blob in blobs:
        if blob.name=='csv_files_with_date_time/':
            continue
        #stringToList(blob.name)
        #print(blob.name)
        filelist.append(blob.name)
        '''
        m=blob.name
        print(m)
        '''
    #print(filelist)
    return filelist
    #print(filelist)




    if delimiter:
        print("Prefixes:")
        for prefix in blobs.prefixes:
            print(prefix)

'''
def stringToList(string):
         return(list(string.split(" ")))
    #m=list(st.append("  "))
        #print(listRes)
    #print(listRes.append(listRes))
'''
#list_blobs_with_prefix("training-demo-project", prefix='csv_file', delimiter='none')
list_blobs_with_prefix("training-demo-project", prefix='csv_files_with_date_time', delimiter='none')







