# CANADAGOOSE ALL PRODUCTS SCRAPER INTO A SPECIFIC CSV FORMAT

## Made a scraper that scrapes all the products from canadagoose.com. Here's how it works:-
1. Firstly, it scrapes all the categories (minor and major) from canadagoose.com
2. It then scrapes all the product urls listed in those categories
3. Scrapes the individual products with all the data; Images (for all colors), Color and size variants, Name, Description, SKU, Type (Simple/Variable), price etc
4. It makes variations if a product is variable (i.e; having variants)

## Techniques involved
1. Reverse Engineering
2. Advanced HTML parsing (getting data from script tags, different attributes associated with some tag)

## How to run and use
1. Use this command in the project directory after downloading the project files:-  ```pip install -r requirements.txt```
2. Then run ```full_scraper.py```
3. It will take some time, and will generate the CSV (like the CSV attached)

## Thank you!
