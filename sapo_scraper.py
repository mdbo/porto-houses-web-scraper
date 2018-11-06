"""
This script is inspired by the great blog post of Fabio Neves:
https://towardsdatascience.com/looking-for-a-house-build-a-web-scraper-to-help-you-5ab25badc83e
"""
import itertools
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Union
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from utils.utils import config_logging, create_dataframe, create_dir, save_df_to_csv

NOW = datetime.now()

SAPO_PORTO_URI = 'https://casa.sapo.pt/Venda/Apartamentos/Porto/'
# sort by date (most recent)
SAPO_FILTERS = '?sa=13&or=10'
N_PAGES = 10
# N_PAGES = 161

OUTPUT_DIR = Path(__file__).parent / 'files'


class SapoScraper:

    def __init__(self, base_uri: str = SAPO_PORTO_URI, filters: str = SAPO_FILTERS,
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
        self.SLEEP = 5

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

    def get_property_listing(self, uri: str = None):
        """Extract the list of properties returned by the search."""
        soup = self._parse(uri)
        if soup:
            listing = soup.find_all('div', class_='searchResultProperty')
            return listing
        else:
            logging.info(f'Failed to get property listing for {uri}')
            return None

    def get_property_info(self, include_filters: bool = True):
        """Get information about each property, namely:
        - Title
        - Zone
        - Price (euros)
        - Area
        - Condition
        - Creation Date
        - Description
        - URI
        """
        titles = []
        zone = []
        prices = []
        areas = []
        condition = []
        created = []
        descriptions = []
        uris = []

        if include_filters:
            uri = urljoin(self.BASE_URI, self.FILTERS_URI)
        else:
            uri = self.BASE_URI

        n_pages = 0
        for page in range(0, self.N_PAGES + 1):
            logging.info(f'Scraping page number {page}')
            n_pages += 1
            sapo_uri = uri + '&pn=' + str(page)
            property_listing = self.get_property_listing(sapo_uri)
            if property_listing:
                for i, property in enumerate(property_listing):
                    logging.info(f'Property {i}')
                    try:
                        # Price
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
                        prices.append(int(price))

                        # Zone
                        location = property.find_all('p', class_="searchPropertyLocation")[0].text
                        location = location[7:location.find(',')]
                        zone.append(location)

                        # Title
                        name = property.find_all('span')[0].text
                        titles.append(name)

                        # Status
                        status = property.find_all('p')[5].text
                        condition.append(status)

                        # Area
                        m2 = property.find_all('p')[9].text
                        if m2 != '-':
                            m2 = m2.replace('\xa0', '')
                            m2 = float("".join(itertools.takewhile(str.isdigit, m2)))
                            areas.append(m2)

                        else:
                            m2 = property.find_all('p')[7].text
                            if m2 != '-':
                                m2 = m2.replace('\xa0', '')
                                m2 = float(''.join(itertools.takewhile(str.isdigit, m2)))
                                areas.append(m2)
                            else:
                                areas.append(m2)

                        # Creation date
                        date = pd.to_datetime(
                            property.find_all('div', class_="searchPropertyDate")[0].text[21:31])
                        created.append(date)

                        # Description
                        desc = property.find_all('p', class_="searchPropertyDescription")[0].text[
                               7:-6]
                        descriptions.append(desc)

                        # URI
                        link = 'https://casa.sapo.pt/' + property.find_all('a')[0].get('href')[1:-6]
                        uris.append(link)
                    except Exception as e:
                        logging.info(f'Failed to extract data for property {i}. Error: '
                                     f'{str(e)}')
                        continue

            else:
                break

            time.sleep(self.SLEEP)

        logging.info(f'{n_pages} pages containing {len(titles)} properties were scraped.')

        return titles, zone, prices, areas, condition, created, descriptions, uris


if __name__ == '__main__':
    config_logging()

    scraper = SapoScraper()
    titles, zone, prices, areas, condition, created, descriptions, uris = scraper.get_property_info(
        include_filters=True)

    logging.info('Organise data into a Pandas DataFrame')
    df = create_dataframe(titles, zone, prices, areas, condition, created, descriptions, uris)

    logging.info('Create directory where the results will be stored')
    create_dir(OUTPUT_DIR)

    csv_filepath = OUTPUT_DIR / f'sapo_porto_properties_{NOW.year}{NOW.month}{NOW.day}.csv'
    logging.info(f'Save results to CSV file: {csv_filepath}')
    save_df_to_csv(df, csv_filepath)
