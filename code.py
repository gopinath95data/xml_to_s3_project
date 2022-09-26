import logging
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import os
import shutil
import boto3

from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

# script configuration
XML_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
DATA_DIRECTORY = './zip_content'
ZIP_FILE_NAME = 'contents.xml'
CSV_DIRECTORY = './csv_file'
CSV_FILE_NAME = 'data.csv'

# AWS CREDENTIALS
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
S3_BUCKET_NAME = ""

#-----------------------#
#### ALL FUNCTIONS ####
#-----------------------#

# Get first xml object that has "DLTINS" as file_type
def get_first_object(root: object) -> object:
    for doc in root.iter('doc'):
        for child in doc:
            if child.tag == "str" and child.attrib["name"] == "file_type":
                if child.text == "DLTINS":
                    return doc

# Get required link based on the given condition
def get_required_link(url: str) -> str:
    response = requests.get(url)
    root = ET.fromstring(response.content)
    req_doc = get_first_object(root)
    
    req_link = ""
    
    for child in req_doc:
        if child.tag == "str" and child.attrib["name"] == "download_link":
            req_link = child.text
    return req_link

# Download required zip file and extract in a local directory
def download_and_extract_content(file_url: str) -> None:
    try:
        shutil.rmtree(f'{DATA_DIRECTORY}')
    except:
        pass
    zipurl = file_url
    with urlopen(zipurl) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            zfile.extractall(DATA_DIRECTORY)
    for filename in os.listdir(DATA_DIRECTORY):
        os.rename(os.path.join(DATA_DIRECTORY,filename), os.path.join(DATA_DIRECTORY,ZIP_FILE_NAME))
    return "File Extracted Successfully"
                    
# Read extracted xml data, parse required data into csv and save locally                   
def write_csv_from_xml_data() -> None:
    xmlparse = ET.parse(f'{DATA_DIRECTORY}/{ZIP_FILE_NAME}')  
    xml_records = []
    
    for child in xmlparse.iter():
        if child.tag.endswith("FinInstrm"):
            xml_records.append(child)
            
    rows_arr = []
    
    for record in xml_records:
        resp = get_data_from_record(record)
        rows_arr.append(resp)
        
    columns = ["Id","FullNm","ClssfctnTp","CmmdtyDerivInd","NtnlCcy","Issr"]
    df = pd.DataFrame(rows_arr,columns=columns)
    try:
        shutil.rmtree(f'{CSV_DIRECTORY}')
    except:
        pass
    os.mkdir(f'{CSV_DIRECTORY}')
    df.to_csv(f'{CSV_DIRECTORY}/{CSV_FILE_NAME}',index=False)

# Get required data from passed xml tag object
def get_data_from_record(record: object) -> list:
    idd,name,tp,ind,ccy,issr = "","","","","",""    
    for child in record.iter():
        if child.tag.endswith('FinInstrmGnlAttrbts'):
            for innerchild in child.iter():
                if innerchild.tag.endswith("Id"):
                    idd = innerchild.text
                if innerchild.tag.endswith("FullNm"):
                    name = innerchild.text
                if innerchild.tag.endswith("ClssfctnTp"):
                    tp = innerchild.text
                if innerchild.tag.endswith("CmmdtyDerivInd"):
                    ind = innerchild.text
                if innerchild.tag.endswith("NtnlCcy"):
                    ccy = innerchild.text
        if child.tag.endswith('Issr'):
            issr = child.text

    return [idd,name,tp,ind,ccy,issr]    

# Read csv file from directory and upload to s3 bucket
def upload_file_to_s3() -> None:
    session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3 = session.resource('s3')
    s3_object = s3.Object(S3_BUCKET_NAME, CSV_FILE_NAME)
    print_and_log("Uploading file to s3. Please wait....")
    result = s3_object.put(Body=open(f'{CSV_DIRECTORY}/{CSV_FILE_NAME}', 'rb'))
    if result["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print_and_log("File uploaded to s3 bucket successfully")
    else:
        print_and_log("File was not uploaded successfully")

def print_and_log(text: str) -> None:
    print(text)
    logging.info(text)

#-----------------------------------#
#### START OF PROGRAM EXECUTION ####
#-----------------------------------#

def main() -> None:
    #log data config
    logging.basicConfig(filename='log_data.log', encoding='utf-8', level=logging.INFO)

    zip_link = get_required_link(XML_URL)
    print_and_log("got required link")
    download_and_extract_content(zip_link)
    print_and_log(f"zip downloaded and extracted into {DATA_DIRECTORY}")
    write_csv_from_xml_data()
    print_and_log(f"data successfully parsed from xml into csv at {CSV_DIRECTORY}/{CSV_FILE_NAME}")
    upload_file_to_s3()


if __name__ == "__main__":
    main()