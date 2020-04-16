# -*- coding: utf-8 -*-
import scrapy
import re

class ImdbSpider(scrapy.Spider):
    name = 'imdb'
    start_urls = ['https://www.imdb.com/title/tt0758758/fullcredits/']
    #start_urls = ["https://www.imdb.com/name/nm0386472/bio"]
    scraped_movies = ["/title/tt0758758/"]

    def parse(self, response):
        movie_id = response.url.split("/")[4]
        title_text = response.css("title::text").extract()[0]
        movie_name = " ".join(title_text.split("-")[0].split(" ")[:-2])
        movie_year = title_text.split("-")[0].split(" ")[-2][1:-1]
        for actor_table in response.css(".cast_list"):
            actors = actor_table.css(".odd") + actor_table.css(".even")
            for actor in actors:
                actor_info = actor.css("td")
                actor_name = actor_info[1].css("::text").extract()[1].strip()
                actor_id = actor_info[1].css('a::attr(href)').extract_first().split("/")[2]
                try:
                    role_name = actor_info[3].css("::text").extract()[1].strip().replace('\n', '')
                except:
                    role_name = actor_info[3].css("::text").extract()[0].strip().replace('\n', '')

                yield{
                    "movie_id": movie_id,
                    "movie_name": movie_name,
                    "movie_year": int(movie_year),
                    "actor_name": actor_name,
                    "actor_id": actor_id,
                    "role_name": role_name
                }

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
        height = table.css("tr")[3]
        height = height.css("td")[1].extract()
        height = height.split("(")[1].split(")")[0][:-2]
        height = float(height)