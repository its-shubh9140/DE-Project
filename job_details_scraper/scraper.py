import csv
import time
import datetime
import re
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import date, datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
#import chromedriver_binary
#from google.cloud import storage
#app = Flask(__name__)
#chrome_options= webdriver.ChromeOptions
#chrome_options.add_argument("--incognito")
#chrome_options = webdriver.ChromeOptions()
"""chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("window-size=1024,768")
chrome_options.add_argument("--no-sandbox")"""
#path="C:\\Users\\Shubham Yadav\\dataEngineering\\chromedriver.exe"
driver=webdriver.Chrome()
def task_scrape() : 
    jobs={"roles":[],
        "companies":[],
        "locations":[],
        "experience":[],
        "skills":[],
        "dates":[],
        "scraper_run_dt_time":[],
        "jd_url":[]
        }
    #This outer loop is for number of pages to be scraped
    for i in range(1,10):
        driver.get("https://www.naukri.com/php-developer-jobs-{}".format(i))
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
            jobs["scraper_run_dt_time"].append(datetime.today())
            jobs["jd_url"].append(jd)
    driver.quit        
    return jobs         

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
    jobs["dates"]=replacing_blank_dates_and_finding_date_from_days_count(jobs["dates"])
    #creating CSV file by appending rows        
    df=pd.DataFrame.from_dict(jobs) #Creating DataFrame
    df.head(10)
    df=df.apply(lambda x: x.astype(str).str.lower()) #converting into lowercase to remove redundancy
    df.head()    
    df.skills=[skill.split("\n") for skill in df.skills]
    df.locations=[location.split(" ") for location in df.locations]
    #df[15:25]
    df.info()
    df.isnull().sum()
    df=df.dropna()
    filename=datetime.today()
    #df.to_csv('jobs.csv')
    return df.to_csv('jobs.csv')
try:
    job_dict=task_scrape()
    job_detals=dictionary_to_df_transformation(job_dict)
    print(job_detals)
except Exception as e:
    print(e)    
    
