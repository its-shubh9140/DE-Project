# Data Collection through Scraper

------

To scrape data fields from the website this scraper is automated by making use of the python's selenium module. Our scraper runs on a VM instance daily to scrape the data fields from the website.

### Work flow

![scraper workflow](https://github.com/its-shubh9140/DE-Project/blob/main/job_details_scraper/VM.PNG)



## To run scraper on VM instance

**This script file will fulfill the necessary dependencies  for the scraper to run on VM.**

```
#!/bin/sh
sudo apt update
sudo apt -y upgrade
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install ./google-chrome-stable_current_amd64.deb
git clone https://github.com/its-shubh9140/DE-Project
pip3 install -r requirements.txt
```

**To run the scraper file one has to SSH into the VM as shown.**

![VM](VM.png)

 **Run the following command in the terminal** .

```
cd DE-Project/job_details_scraper
python3 scraper.py
```

