import time
import json
import pandas as pd
import re
import logging
from google.cloud import storage
from datetime import date,datetime,timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
 
def replacing_blank_dates_and_finding_date_from_days_count(jobs):
    filtered_data=[]  
    for value in jobs:
        if not value:
            filtered_data.append(str(date.today()))       
        else:
            for item in value: 
                integer_value=int(str(item))
                dt=date.today()-timedelta(integer_value)
                filtered_data.append(str(dt))
    return filtered_data  

def dictionary_to_df_transformation(jobs):      
    df=pd.DataFrame.from_dict(jobs) #Creating DataFrame
    df.head(10)
    df=df.apply(lambda x: x.astype(str).str.lower()) #converting into lowercase to remove redundancy
    df.head()    
    df.skills=[skill.split("\n") for skill in df.skills]
    df.locations=[location.split(" ") for location in df.locations]
    df=df.dropna()
    return df

#Method to upload csv file to Bucket
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    x=datetime.datetime.now().replace(microsecond=0)

    logging.debug(
        "File {} uploaded to {} at {}.".format(
            source_file_name,destination_blob_name,str(x)
        )
    )

def task_scrape():
    with open("config.json") as file:
        data=json.load(file)
        service=Service(data["path"])
        driver = webdriver.Chrome(service=service)
        jobs={"roles":[],
            "companies":[],
            "locations":[],
            "experience":[],
            "skills":[],
            "dates":[],
            "scraper_run_dt_time":[],
            "jd_url":[]
            }
        final_df=pd.DataFrame(jobs)    
        #This outer loop is for number of pages to be scraped
        for role in data["job_roles"]:
            jobs={key:[] for key in jobs}
            #print("hello",jobs)
            for i in range(1,3):
                driver.get("https://www.naukri.com/{}-jobs-{}".format(role,i))
                time.sleep(4)
                job_container = driver.find_elements(By.CSS_SELECTOR,".jobTuple.bgWhite.br4.mb-8")
                # scraping the details from webpage
                for job in job_container:
                    driver.implicitly_wait(20)
                    role=job.find_element(By.CSS_SELECTOR,"a.title.fw500.ellipsis").text
                    company=job.find_element(By.CSS_SELECTOR,"a.subTitle.ellipsis.fleft").text
                    location=job.find_element(By.CSS_SELECTOR,".fleft.grey-text.br2.placeHolderLi.location").text
                    try:
                        exp=job.find_element(By.CSS_SELECTOR,".fleft.grey-text.br2.placeHolderLi.experience").text
                    except Exception:
                        exp="0 yrs"      
                    skills=job.find_element(By.CSS_SELECTOR,".tags.has-description").text
                    date_string=job.find_element(By.CSS_SELECTOR,"[class^='type br2 fleft']").text  
                    # date_string contains strings like 2 day ago,just now,few hours ago                                                                                
                    jd=job.find_element(By.TAG_NAME,"a").get_attribute("href")
                    date=re.findall(r'\d+',date_string) #extracting numbers out of the date_string
                    jobs["roles"].append(role)
                    jobs["companies"].append(company)
                    jobs["locations"].append(location)
                    jobs["experience"].append(exp)
                    jobs["skills"].append(skills)
                    jobs["dates"].append(date)
                    jobs["jd_url"].append(jd)
                    jobs["scraper_run_dt_time"].append(datetime.today())                   
            jobs["dates"]=replacing_blank_dates_and_finding_date_from_days_count(jobs["dates"])
            try:
                print(type(jobs))
                dataframe=dictionary_to_df_transformation(jobs)
                print(type(dataframe))
                dataframe[15:25]
                final_df=final_df.append(dataframe)
            except:
                logging.error("Error in dict_to_df")                
        now=datetime.today()
        dt_time=now.strftime("%H%M%S")
        dt=now.strftime("%Y%m%d")
        filename="scraped_"+dt+"_"+dt_time
        final_df.info()
        final_df[40:45]
        driver.quit
        final_df.to_csv('{}.csv'.format(filename))
        return filename
try:
    scraped_file=task_scrape()
    upload_blob("staging_data_scraper",scraped_file,scraped_file)
except Exception as e:
    logging.error(e)    

