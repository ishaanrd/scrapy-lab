# -*- coding: utf-8 -*-
import scrapy
import sys
sys.path.extend("/Users/trapti/PycharmProjects/lab8/venv/lib/python3.7/site-packages")
import unidecode
import re

cleanString = lambda x: '' if x is None else unidecode.unidecode(re.sub(r'\s+',' ',x))

class NytimesSpider(scrapy.Spider):
    name = 'nytimes'
    allowed_domains = ['www.nytimes.com']
    start_urls = ['https://www.nytimes.com/']

    def parse(self, response):
        for section in response.css("section[data-testid]"):
            section_name = section.attrib['data-block-tracking-id']
            for article in section.css("article"):
                article_url = article.css('a::attr(href)').extract_first()
                yield {
                    'section': section_name,
                    'appears_url': response.url,
                    'title': cleanString(article.css('a h2::text, a h2 span::text').extract_first()),
                    'article_url': response.url[:-1]+str(article_url or ""),
                    'summary': cleanString(''.join(article.css('p::text, ul li::text').extract())),
                }
                next_page = article_url
                if next_page is not None:
                    yield response.follow(next_page, callback=self.parse_article)

    def parse_article(self, response):
        yield {
            'appears_url': response.url,
            'title': cleanString(response.css("h1[itemprop='headline'] span::text, h1[itemprop='headline']::text").extract_first()),
            'authors': cleanString(', '.join(response.css("p[itemprop='author'] a span[itemprop='name']::text").extract())),
            'contents': cleanString(''.join(response.css('section[itemprop=\'articleBody\'] p::text').extract())),
        }
