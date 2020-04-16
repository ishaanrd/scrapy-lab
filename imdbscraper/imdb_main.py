from scrapy import cmdline

cmdline.execute("scrapy crawl imdb -o imdb.json".split())