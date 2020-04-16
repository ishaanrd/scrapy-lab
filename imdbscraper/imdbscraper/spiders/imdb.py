# -*- coding: utf-8 -*-
import scrapy
import re
from elasticsearch import Elasticsearch
import os
from uuid import uuid4

ELASTIC_API_URL_HOST = os.environ['ELASTIC_API_URL_HOST']
ELASTIC_API_URL_PORT = os.environ['ELASTIC_API_URL_PORT']
ELASTIC_API_USERNAME = os.environ['ELASTIC_API_USERNAME']
ELASTIC_API_PASSWORD = os.environ['ELASTIC_API_PASSWORD']

es=Elasticsearch(host=ELASTIC_API_URL_HOST,
                 scheme='https',
                 port=ELASTIC_API_URL_PORT,
                 http_auth=(ELASTIC_API_USERNAME,ELASTIC_API_PASSWORD))

class ImdbSpider(scrapy.Spider):
    name = 'imdb'
    start_urls = ['https://www.imdb.com/title/tt0081505/fullcredits/']
    scraped_movies = ["/title/tt0081505/"]
    scraped_actors = []
    actor_name=""
    def parse(self, response):
        movie_id = response.url.split("/")[4]
        #title_text = response.css("title::text").extract()[0]
        movie_name = response.css('.parent a::text').extract_first()
        movie_year = response.css('.nobr::text').extract_first().strip()[1:5]
        for actor_table in response.css(".cast_list"):
            actors = actor_table.css(".odd") + actor_table.css(".even")
            for actor in actors:
                actor_info = actor.css("td")
                self.actor_name = actor_info[1].css("::text").extract()[1].strip()
                actor_id = actor_info[1].css('a::attr(href)').extract_first().split("/")[2]
                try:
                    role_name = actor_info[3].css("::text").extract()[1].strip().replace('\n', '')
                except:
                    role_name = actor_info[3].css("::text").extract()[0].strip().replace('\n', '')

                yield{
                    "movie_id": movie_id,
                    "movie_name": movie_name,
                    "movie_year": (movie_year),
                    "actor_name": self.actor_name,
                    "actor_id": actor_id,
                    "role_name": role_name
                }
                es.index(index='imdb1_movies',
                         doc_type='movies',
                         #id=uiid4(),
                         body={
                             "movie_id": movie_id,
                             "movie_name": movie_name,
                             "movie_year": (movie_year),
                             "actor_name": self.actor_name,
                             "actor_id": actor_id,
                             "role_name": role_name
                         })

                if actor_id not in self.scraped_actors:
                    self.scraped_actors.append(actor_id)
                    actor_bio = f"https://www.imdb.com/name/{actor_id}/bio/"
                    yield response.follow(actor_bio,callback=self.parse_bio)

                actor_url = f"https://www.imdb.com/name/{actor_id}/"
                yield response.follow(actor_url, callback=self.parse_actor)

    def parse_actor(self, response):
        movie_ids = []
        films_category = response.css(".filmo-category-section")
        films = films_category.css("div")
        film_ids = films.css("b")
        film_ids = film_ids.css('a::attr(href)').extract()
        film_years = films.css("span")
        film_years = film_years.css("::text").extract()
        film_years = [re.sub("[^0-9]", "", year) for year in film_years];

        for i in range(len(film_ids)):
            try:
                film_year = int(film_years[i])
            except:
                film_year = 9999

            if 1980 <= film_year <= 1989:
                movie_ids.append(film_ids[i])

        for movie_id in movie_ids:
            if movie_id not in self.scraped_movies:
                self.scraped_movies.append(movie_id)
                film_url = f"https://www.imdb.com{movie_id}fullcredits/"
                yield response.follow(film_url, callback=self.parse)

    def parse_bio(self, response):
        table = response.css("#overviewTable")
        birth_year = table.css("tr")[0]
        birth_year = birth_year.css("td")[1]
        birth_year = birth_year.css("a")[1]
        birth_year = birth_year.css("::text").extract_first()
        birth_year = int(birth_year)

        height = table.css("tr")[3]
        height = height.css("td")[1].extract()
        height = height.split("(")[1].split(")")[0][:-2]
        height = float(height)

        es.index(index='imdb1_bio',
                 doc_type='actor_bio',
                 # id=uiid4(),
                 body={
                     "actor_name" : self.actor_name,
                     "actor_height": float(height),
                     "actor_age": int(1985-birth_year)
                 })
