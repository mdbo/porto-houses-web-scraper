"""
This script is inspired by Fabio Neves' Medium blog post:
https://towardsdatascience.com/looking-for-a-house-build-a-web-scraper-to-help-you-5ab25badc83e
"""
import itertools
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Tuple, Union
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from utils.utils import config_logging, create_dataframe, create_dir, save_df_to_csv

NOW = datetime.now()

CASA_SAPO_URI = 'https://casa.sapo.pt'
CASA_SAPO_FILTERS = '/Venda/Apartamentos/Porto/?sa=13&or=10'
N_PAGES = 1

OUTPUT_DIR = Path(__file__).parent / 'files'


def _preprocess_m2(result: str):
    m2 = result.replace('\xa0', '')
    return float("".join(itertools.takewhile(str.isdigit, m2)))


class CasaSapoScraper:

    def __init__(self, base_uri: str = CASA_SAPO_URI, filters: str = CASA_SAPO_FILTERS,
                 n_pages: int = N_PAGES):
        """Web scraper that gathers property data from casa.sapo.pt online real estate database.

        :param base_uri: the base URI
        :param filters: filters to add to the base URI
        :param n_pages: number of pages returned by the search
        """
        self.BASE_URI = base_uri
        self.FILTERS_URI = filters
        self.N_PAGES = n_pages
        self.HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, '
                                      'like Gecko) Chrome/41.0.2228.0 Safari/537.36'}
        self.SLEEP = 7

    def _parse(self, uri: str = None, headers: dict = None) -> Union[BeautifulSoup, None]:
        """Query the URI and parse the HTML using BeautifulSoup."""
        try:
            page = requests.get(uri, headers=headers if headers else self.HEADERS)
            page.raise_for_status()
            soup = BeautifulSoup(page.text, 'html.parser')
        except requests.exceptions.HTTPError as e:
            logging.info(f'HTTP Error: {str(e)}')
            return None
        except requests.exceptions.ConnectionError as e:
            logging.info(f'Error Connecting: {str(e)}')
            return None
        except requests.exceptions.Timeout as e:
            logging.info(f'Timeout Error: {str(e)}')
            return None
        except requests.exceptions.RequestException as e:
            logging.info(f'Oops, something else: {str(e)}')
            return None

        return soup

    @staticmethod
    def get_property_price(property: BeautifulSoup) -> Union[int, None]:
        """Get the price of a property."""
        try:
            price = property.find_all('span')[2].text
            if price == 'Contacte Anunciante':
                price = property.find_all('span')[3].text
                if price.find('/') != -1:
                    price = price[0:price.find('/') - 1]
            if price.find('/') != -1:
                price = price[0:price.find('/') - 1]

            price_ = [int(price[s]) for s in range(0, len(price)) if price[s].isdigit()]
            price = ''
            for x in price_:
                price = price + str(x)
            return int(price)
        except Exception as e:
            logging.info(f'Failed to extract property price. Error: {str(e)}')
            return None

    @staticmethod
    def get_property_zone(property: BeautifulSoup) -> str:
        """Get the zone/location of a property."""
        try:
            location = property.find_all('p', class_="searchPropertyLocation")[0].text
            location = location[7:location.find(',')]
            return location
        except Exception as e:
            logging.info(f'Failed to extract the property zone. Error: {str(e)}')
            return ''

    @staticmethod
    def get_property_title(property: BeautifulSoup) -> str:
        """Get the title of the property as listed in casa.sapo.pt."""
        try:
            return property.find_all('span')[0].text
        except Exception as e:
            logging.info(f'Failed to extract the property title. Error: {str(e)}')
            return ''

    @staticmethod
    def get_property_condition(property: BeautifulSoup) -> str:
        """Get the property condition."""
        try:
            return property.find_all('p')[5].text
        except Exception as e:
            logging.info(f'Failed to extract the property condition. Error: {str(e)}')
            return ''

    @staticmethod
    def get_property_size(property: BeautifulSoup) -> str:
        """Get the property area/size (square metre)."""
        try:
            m2 = property.find_all('p')[9].text
            if m2 != '-':
                m2 = _preprocess_m2(m2)
            else:
                m2 = property.find_all('p')[7].text
                if m2 != '-':
                    m2 = _preprocess_m2(m2)
            return m2
        except Exception as e:
            logging.info(f'Failed to extract the property area/size. Error: {str(e)}')
            return ''

    @staticmethod
    def get_property_date(property: BeautifulSoup) -> str:
        """Get the date where the property was added to casa.sapo.pt."""
        try:
            return pd.to_datetime(
                property.find_all('div', class_="searchPropertyDate")[0].text[21:31])
        except Exception as e:
            logging.info(f'Failed to extract the property date. Error: {str(e)}')
            return ''

    @staticmethod
    def get_property_description(property: BeautifulSoup) -> str:
        """Get the property description."""
        try:
            return property.find_all('p', class_="searchPropertyDescription")[0].text[7:-6]
        except Exception as e:
            logging.info(f'Failed to extract the property description. Error: {str(e)}')
            return ''

    def get_property_link(self, property: BeautifulSoup) -> str:
        """Get the property link (uri) in casa.sapo.pt."""
        try:
            return urljoin(self.BASE_URI, property.find_all('a')[0].get('href')[1:-6])
        except Exception as e:
            logging.info(f'Failed to extract the property link. Error: {str(e)}')
            return ''

    def get_property_info(self, property: BeautifulSoup) -> Tuple[str]:
        """Get information for a specific property, namely:
        - Title
        - Price (€)
        - Area/Size (m2)
        - Zone
        - Condition
        - Date
        - Description
        - URI/Link
        """
        title = self.get_property_title(property)
        price = self.get_property_price(property)
        size = self.get_property_size(property)
        zone = self.get_property_zone(property)
        condition = self.get_property_condition(property)
        date = self.get_property_date(property)
        description = self.get_property_description(property)
        link = self.get_property_link(property)

        return title, price, size, zone, condition, date, description, link

    def get_property_listing(self, uri: str = None) -> Union[BeautifulSoup, None]:
        """Extract the list of properties returned by the search."""
        soup = self._parse(uri)
        if soup:
            listing = soup.find_all('div', class_='searchResultProperty')
            return listing
        else:
            logging.info(f'Failed to get property listing for {uri}')
            return None

    def get_all_properties(self, include_filters: bool = True) -> pd.DataFrame:
        """Get information for all properties found on a specific search (with or without
        filters) and save it in a Pandas DataFrame."""

        results = []

        uri = self.BASE_URI
        if include_filters:
            uri = urljoin(self.BASE_URI, self.FILTERS_URI)

        for page_number in range(0, self.N_PAGES):
            logging.info(f'Scraping page number {page_number}')
            page_uri = uri + '&pn=' + str(page_number)
            property_listing = self.get_property_listing(page_uri)
            if property_listing:
                try:
                    for i, property in enumerate(property_listing):
                        logging.info(f'Property {i}')
                        result = self.get_property_info(property)
                        results.append(result)
                except Exception as e:
                    logging.info(f'Failed to extract data for property {i}. Error: '
                                 f'{str(e)}')
                    continue
            else:
                break

            time.sleep(self.SLEEP)

        logging.info(f'Scraped {self.N_PAGES} pages containing {len(results)} properties.')

        logging.info('Organise data into a Pandas DataFrame')
        data = create_dataframe(*list(zip(*results)))
        return data


if __name__ == '__main__':
    config_logging()

    scraper = CasaSapoScraper()
    df = scraper.get_all_properties(include_filters=True)

    logging.info('Create directory where the results will be stored')
    create_dir(OUTPUT_DIR)

    csv_filepath = OUTPUT_DIR / f'sapo_porto_properties_{NOW.year}{NOW.month}{NOW.day}.csv'
    logging.info(f'Save results to CSV file: {csv_filepath}')
    save_df_to_csv(df, csv_filepath)
