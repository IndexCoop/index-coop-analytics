from selenium import webdriver
from selenium.webdriver.common.keys import Keys

"""
Prereqs:
    - Install selenium python package (pip install selenium)
    - Download chromedriver version matching your chrome browser: https://chromedriver.chromium.org/downloads
"""


vote_urls = {
    'BTC2X-FLI': 'https://snapshot.org/#/index/proposal/Qmc2DPHoKnyYxRjQfwbpbBngt5xHZrLBgkywGqZm7hHnEU',
    'SMI': 'https://snapshot.org/#/index/proposal/QmYBG5zerdSkC9TGwguy5biCS5h2cg49PQCHCukJqHmfE1',
    'TTI': 'https://snapshot.org/#/index/proposal/QmTPVkgfJBB1go2DCFhmacjgSWrLUzTFimdTGdB7d9Q2ao',
    'MVI': 'https://snapshot.org/#/index/proposal/QmadsabYMJC96jU2S2kPCSh1suVfDVApGLwrux2WwsHd7x',
    'FLI2': 'https://snapshot.org/#/index/proposal/QmYHV2vdTaSubtNJefSoYx82ypsmtzrT7CGUU1EHsXzHC3',
    'FLI1': 'https://snapshot.org/#/index/proposal/QmQwQn4k324kMKPjsSX6ZEzjkkKWh1DNfAN2mQ3dd5aP1a'
}

def scrape_vote_csv_reports(vote_url:str):
    """
    Automated download of CSV report for Snapshot vote, given url
    """
    driver = webdriver.Chrome(executable_path='C:/Users/craig/IndexCoop/chromedriver.exe')
    driver.get(vote_url)
    driver.implicitly_wait(10)
    download_snapshot_vote_report_button = driver.find_element_by_xpath('//button[.="Download report"]')
    download_snapshot_vote_report_button.click()

for vote_url in vote_urls.values():
    print(f'Fetching {vote_url}')
    scrape_vote_csv_reports(vote_url)
