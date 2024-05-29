# server.py
import subprocess

from flask import Flask
from scrapy.crawler import CrawlerRunner

app = Flask(__name__)

@app.route('/run/<spider>')
def run_spider(spider):
    subprocess.call(['scrapy', 'crawl', spider])
    return "OK"

if __name__=='__main__':
    app.run('0.0.0.0', 8080)