# XML to s3 Project
Python Project to parse data based on specific conditions and load the final csv data to s3.

### Requirements:
  1. Download the xml from this [link](https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100)
  2. From the xml, please parse through to the first download link whose file_type is DLTINS and download the zip
  3. Extract the xml from the zip.
  4. Convert the contents of the xml into a CSV with the following header:
      FinInstrmGnlAttrbts.Id
      FinInstrmGnlAttrbts.FullNm
      FinInstrmGnlAttrbts.ClssfctnTp
      FinInstrmGnlAttrbts.CmmdtyDerivInd
      FinInstrmGnlAttrbts.NtnlCcy
      Issr
  5. Store the csv from step 4) in an AWS S3 bucket
  6. The above function should be run as an AWS Lambda (Optional)

### Instructions:
  1. Install requirements from requirements.txt file
  2. Configure aws credentials inside code.py (access key id and secret access key)
  3. To run script, use "python code.py"
  4. To run test, use "python -m unittest code_test.py"
 
