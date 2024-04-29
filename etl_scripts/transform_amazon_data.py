import json

def main():

    print('Data Cleaning and Transformation Started!')

    # Initialize the file path
    file_path = r'/home/raufhamidy/Documents/amazon_etl_pipeline/products_data.json'

    # Clean the data and floatify the price and rating
    print('Cleaning and Floatifying the Price and Rating Data...')
    clean_floatify(file_path)

    # Clean the data and intify the price and rating
    print('Cleaning and Intifying the Price and Rating Data...')
    clean_intify(file_path)

    print('Data Cleaning and Transformation Completed!')

def clean_floatify(file_path):
    
    # Load the data
    with open(file_path, 'r', encoding='utf-8') as f:
        products_data = json.load(f)

    # Iterate over the data
    for item in products_data:

        # If Offers bool is True, then clean the offers
        offers_bool = item['offers_bool']

        # Clean price
        if offers_bool == True:
            price = item['price']

            if 'from' in price:
                price = price.split('from ')[1]
                price = price.replace('$', '')
                price = price.replace(',', '')
                item['price'] = price

            else:
                price = item['price']
                price = price.replace('$', '')
                price = price.replace(',', '')
                price = float(price)
                item['price'] = price
                
        else:
            price = item['price']
            price = price.replace('$', '')
            price = price.replace(',', '')
            price = float(price)
            item['price'] = price

        # Clean rating
        rating = item['rating']
        rating = rating.split(' out')[0]
        rating = float(rating)
        item['rating'] = rating

    # Save the data back to a file
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(products_data, f, indent=4)

    print('Price and Rating Data cleaned and saved to', file_path)

def clean_intify(file_path):

     # Load the data
    with open(file_path, 'r', encoding='utf-8') as f:
        products_data = json.load(f)

    # Iterate over the data
    for item in products_data:
        
        # Clean Offers
        offers_bool = item['offers_bool']
        if offers_bool == True:
            offers = item['offers']
            offers = offers.split(' offer')[0]
            offers = int(offers)
            item['offers'] = offers

        # Clean Rank
        rank = item['rank']
        rank = rank.replace('#', '')
        rank = int(rank)
        item['rank'] = rank

        # Clean Rating Volume
        rating_volume = item['rating_volume']
        rating_volume = rating_volume.replace(',', '')
        rating_volume = int(rating_volume)
        item['rating_volume'] = rating_volume

    # Save the data back to a file
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(products_data, f, indent=4)

    print('Offers, Rank, and Rating Volume Data cleaned and saved to', file_path)

if __name__ == '__main__':
    main()
