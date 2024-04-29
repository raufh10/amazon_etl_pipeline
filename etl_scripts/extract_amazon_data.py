from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import os
import re
import json
import time
import random

# Main Function
def main():

    print('Starting Amazon Data Extraction...')

    # Initialize File Path
    file_path = r'/home/raufhamidy/Documents/amazon_etl_pipeline/products_data.json'

    print('Extracting best_seller list...')
    extract_product_list(file_path)
    print('Best_seller list extracted.')

    print('Extracting product brand...')
    extract_product_brand(file_path)
    print('Product brand extracted.')

    print('Amazon Data Extraction Complete.')

def loading_page(page, url, toggle=False):

    print('Loading page...')

    # Max attempts
    max_attempts = 3

    # For loop to try loading page
    for i in range(max_attempts):
        print(f'Attempt {i+1}')

        try:
            # Page Load
            page.goto(url, timeout=600000)
            page.wait_for_load_state()
            time.sleep(random.randint(10, 15)) # Most of the times it triggers captcha, manually solve it while the program is sleeping
            
            if toggle:
                time.sleep(6000000)

            page.click('#zg_banner_subtext') # Click on the banner
            
            # Check if page is loaded
            if page.wait_for_selector('#nav-logo-sprites'): # Using the Amazon logo as a selector to check if page is loaded

                for _ in range(20):
                    page.keyboard.press('PageDown')
                    time.sleep(0.5)

                page.keyboard.press('End')
                page.keyboard.press('Home')
                page.wait_for_load_state()

                print('Page loaded')
                break

            # If page is not loaded, try again
            else:
                print('Page not loaded')
                time.sleep(1)
                continue

        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(5)
            continue

def turn_json_into_list_of_dicts(file_path):

    print(f'Turning {file_path} into a list of dictionaries...')

    # Turn JSON file into a list of dictionaries
    list_of_dicts = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            dict_data = json.loads(line)
            list_of_dicts.append(dict_data)

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(list_of_dicts, f, indent=4, ensure_ascii=False)

    print(f'{file_path} has been turned into a list of dictionaries.')

def extract_product_list(file_path):

    # Playwright sequence
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')
        page = context.new_page()

        # Urls of top 100 best seller gaming pc mice pages
        best_seller_pages = [
            "https://www.amazon.com/best-sellers-video-games/zgbs/videogames/402052011/ref=zg_bs_pg_1_videogames?_encoding=UTF8&pg=1",
            "https://www.amazon.com/best-sellers-video-games/zgbs/videogames/402052011/ref=zg_bs_pg_2_videogames?_encoding=UTF8&pg=2"
        ]

        # Starting URL to initiate the scraping
        starting_url = 'https://www.upwork.com/nx/jobs/search/?nbs=1&q=data%20analyst&per_page=50'
        page.goto(starting_url, timeout=600000)

        # For loop pages in best_seller_pages and scrape product_page_urls and product_data
        for url in best_seller_pages:
            loading_page(page, url, False)

            try:
                extract_job_list_data(page, file_path)
            except Exception as e:
                print(f"An error occurred: {e}")
                time.sleep(1)
                continue

        # Turn job_data into a list of dictionaries
        file_path_job_data = file_path
        turn_json_into_list_of_dicts(file_path_job_data)
        
        # Close Playwright
        browser.close()

def extract_job_list_data(page, file_path):

    # Parse HTML
    soup = BeautifulSoup(page.content(), "lxml")

    # Get catalogue and container using CSS selectors
    catalogue_container = soup.find('div', {'class': 'p13n-gridRow _cDEzb_grid-row_3Cywl'}) # Div tag of the product catalogue
    product_cards = catalogue_container.findAll('div', {'class': 'a-cardui _cDEzb_grid-cell_1uMOS expandableGrid p13n-grid-content'}) # CSS selector of each product card

    # For loop product_cards and scrape product_page_urls and product_data
    for product_card in product_cards:

        product_dict = {}

        # Extract the title

        # These are the variance CSS selector of the title tag
        title_classes = [
                '_cDEzb_p13n-sc-css-line-clamp-3_g3dy1',
                '_cDEzb_p13n-sc-css-line-clamp-4_2q2cc',
                '_cDEzb_p13n-sc-css-line-clamp-1_1Fn1y'
            ]
        # For loop title_classes to find the title tag
        for title_class in title_classes:
                title_tag = product_card.find('div', {'class': title_class})
                if title_tag:
                    title = title_tag.text.strip()

        # Extract the product ID from the product URL
        id_tag = product_card.find('div', {'class': 'p13n-sc-uncoverable-faceout'})
        product_id = id_tag.get('id')

        # Extract the URL
        product_title_a_tag = id_tag.find('a', {'class': 'a-link-normal'})
        product_url = f"https://www.amazon.com{product_title_a_tag['href']}"

        # Get price and offers

        # These are the variance CSS selector of the price tag
        price_tag_classes = [
            'a-size-base a-color-price', # Price tag when there is no offer
            'a-color-secondary' # Price tag when there is an offer
        ]

        # For loop price_tag_classes to find the price tag
        for tag_class in price_tag_classes:
            price_tag = product_card.find('span', {'class': tag_class})
            
            # If price_tag is found, extract the price
            if price_tag:
                price = price_tag.text.strip()

                # If there is an offer, remove the offer text
                starting_price = re.search(r'\d+ offers from ', price) # Regex to find the offer text
                if starting_price:
                    price = re.sub(r'\d+ offers from ', '', price)
            
                # If there is an offer, extract the offer
                if tag_class == 'a-color-secondary':
                    try:
                        offers_child_tag = product_card.find('span', {'class': 'a-color-secondary'})
                        offers = offers_child_tag.find_parent('a').text.strip()
                        offers_bool = True
                    except:
                        pass
                else:
                    offers = None
                    offers_bool = False        

        # Extract the product rank
        rank_tag = product_card.find('span', {'class': 'zg-bdg-text'})
        rank = rank_tag.text.strip()

        # Extract Product Rating
        rating_tag = product_card.find('span', {'class': 'a-icon-alt'})
        rating = rating_tag.text.strip()

        # Extract the product rating volume
        rating_volume_tag = product_card.find('span', {'class': 'a-size-small'})
        rating_volume = rating_volume_tag.text.strip()

        # Store product data in product_dict
        product_dict['asin'] = product_id
        product_dict['title'] = title
        product_dict['product_url'] = product_url
        product_dict['price'] = price
        product_dict['offers'] = offers
        product_dict['offers_bool'] = offers_bool
        product_dict['rank'] = rank
        product_dict['rating'] = rating
        product_dict['rating_volume'] = rating_volume

        # Store the data in a JSON file
        with open(file_path, 'a', encoding='utf-8') as json_file:
            json.dump(product_dict, json_file, ensure_ascii=False)
            json_file.write('\n')

def extract_product_brand(file_path):
    
    # Turn JSON file into a list of dictionaries
    with open(file_path, 'r', encoding='utf-8') as f:
        list_of_dicts = json.load(f)

    # Extract brand from title
    for item in list_of_dicts:
        title = item['title']
        brand = title.split()[0]
        brand.capitalize()
        item['brand'] = brand

    # Write the data back to the JSON file
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(list_of_dicts, f, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    main()
    