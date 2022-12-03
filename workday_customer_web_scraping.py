# script to scrape customer names from workday customer website.
# to get complete list you need to click the "loadmore" button
# so to automate that we need selenium which automates a browser.
# if using this script, you need to download chromedriver and put in in you home directory

from selenium import webdriver
from os.path import expanduser
import time
import csv

# URL for workday list of customers page
WORKDAY_CUSTOMER_PAGE_URL = 'https://www.workday.com/en-us/customers.html#?q='

# load the chromedriver, stored in your homedir
home_dir = expanduser("~")
browser = webdriver.Chrome(executable_path=f'{home_dir}/chromedriver')

# load the web page in the automated browser
browser.get(WORKDAY_CUSTOMER_PAGE_URL)

# click the "load more" button until you load all available customers
while True:
    try:
        loadMoreButton = browser.find_elements_by_xpath(
            "//button[@class='load-more has-arrow hide-loadmore-off']"
        )[0]
        time.sleep(2)
        loadMoreButton.click()
        time.sleep(3)
    except Exception as e:
        print(e)
        break
print("Completed loading of all customer pages")

# search for customer name text in span, iterate over items and extract text
customer_element = browser.find_elements_by_xpath("//span[@class='m-tile ng-binding']")
customers = [item.get_attribute('textContent') for item in customer_element]

# the following customers had odd text values, needed to re-name
translations = {
    'HR Digital Transformation Infographic | Highmark Health | Workday': 'Highmark Health',
    'Workday and CarMax | Read Customer Success Stories': 'CarMax',
    'Aurecon Gains Key Insights Infographic | Workday': 'Aurecon',
    'Workday and The Salvation Army | Read Customer Success Stories': 'The Salvation Army'
}

with open('workday_customers.csv', 'w') as f:
    writer = csv.writer(f)
    for val in map(lambda x: translations.get(x, x), customers):
        writer.writerow([val])
