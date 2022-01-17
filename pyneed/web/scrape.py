import urllib

import requests
from loguru import logger
from requests_html import HTMLSession


class GoogleScraper:
    def __init__(self):
        self.session = HTMLSession()

    def get_results(self, query):
        query = urllib.parse.quote_plus(query)
        response = self.get_source("https://www.google.co.uk/search?q=" + query)

        return response

    def parse_results(self, response):

        css_identifier_result = ".tF2Cxc"
        css_identifier_title = "h3"
        css_identifier_link = ".yuRUbf a"
        css_identifier_text = ".IsZvec"

        results = response.html.find(css_identifier_result)

        for result in results:
            item = {
                'title': result.find(css_identifier_title, first=True).text,
                'link': result.find(css_identifier_link, first=True).attrs['href'],
                'snippet': result.find(css_identifier_text, first=True).text
            }

            yield item

    def __call__(self, query):
        response = self.get_results(query)
        return self.parse_results(response)

    def get_source(self, url):
        """Return the source code for the provided URL.

        Args:
            url (string): URL of the page to scrape.

        Returns:
            response (object): HTTP response object from requests_html.
        """

        try:
            response = self.session.get(url)
            return response
        except requests.exceptions.RequestException as e:
            logger.error(e)