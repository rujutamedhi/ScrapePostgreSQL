import traceback
import csv
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import psycopg2


# Database setup
db_config = {
    "dbname": "scraped_data",
    "user": "postgres",
    "password": "rujutamedhi@04",
    "host": "localhost",
    "port": "5432"
}

try:
    #To interact with database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    print("Connected to the database!")

    # Create table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS twitter_data (
        id SERIAL PRIMARY KEY,
        bio TEXT,
        following_count TEXT,
        follower_count TEXT,
        location TEXT,
        website TEXT
    )
    """)
    print("Table created successfully")

except Exception as e:
    print("Error connecting to database:", e)

# Selenium WebDriver setup
chrome_options = Options()
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)



# Open CSV file
with open('C:\\projects\\ScrapePostgreSQL\\twitter_links.csv', mode='r', newline='', encoding='utf-8') as file:
    csv_reader = csv.reader(file)
    first_itr = True

    for row in csv_reader:
        url = row[0]
        print(f"Scraping URL: {url}")

        try:
            driver.get(url)
            if first_itr:
                time.sleep(100)  # Pause for login
                first_itr = False
            time.sleep(15)

            WebDriverWait(driver, 50).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'body'))
            )
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            box = soup.find("div", class_="css-175oi2r r-3pj75a r-ttdzmv r-1ifxtd0")
            findall = box.find_all("span", class_="css-1jxf684 r-bcqeeo r-1ttztb7 r-qvutc0 r-poiln3")

            target_classes = ["css-1jxf684", "r-bcqeeo", "r-1ttztb7", "r-qvutc0", "r-poiln3"]
            filtered_items = []

            # Remove unwanted elements
            for item in findall:
                classes = item.get('class', [])
                if sorted(classes) == sorted(target_classes) and ("Translate bio" not in item.text or "." not in item.text):
                    filtered_items.append(item)

            # Scrape bio if available
            bio_info = filtered_items[-10].text.strip() if len(filtered_items) > 9 else "Not available"
            

            # Scrape following count if available
            following = findall[-5].text.strip() if len(findall) > 4 else "Not available"
            

            # Scrape follower count if available
            follower = findall[-3].text.strip() if len(findall) > 2 else "Not available"
            

            # Scrape location if available
            loc = findall[-9].text.strip() if len(findall) > 8 else "Not available"
           

            # Scrape website if available
            link = box.find('a', class_="css-1jxf684 r-bcqeeo r-1ttztb7 r-qvutc0 r-poiln3 r-4qtqp9 r-1a11zyx r-1loqt21")
            
            website=link.get('href').strip() if link else "not available"
            # To insert scraped data into the database
            insert_query = """
            INSERT INTO twitter_data (bio, following_count, follower_count, location, website)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (bio_info, following, follower, loc, website))
            conn.commit()

        except Exception as e:
            print("An error occurred:", e)
            print(traceback.format_exc())


# Close the database connection and Selenium WebDriver
conn.close()
driver.quit()
