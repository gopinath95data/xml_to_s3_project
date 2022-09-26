import unittest
from code import download_and_extract_content, get_required_link
from xml.etree.ElementTree import XML

XML_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
test1_result = "http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip"
test2_result = "File Extracted Successfully"

class TestFunctions(unittest.TestCase):
    def test_link(self):
        self.assertEqual(get_required_link(XML_URL),test1_result)
    def test_file(self):
        self.assertEqual(download_and_extract_content(test1_result),test2_result)
