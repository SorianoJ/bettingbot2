import scrapy
from scrapy.http import Request, Response
import json
from datetime import datetime, timedelta
from bettingsbot.items import UnibetItem

URL = 'https://eu-offering.kambicdn.org/offering/v2018/ub/listView/football/all/all/all/starting-within.json?lang=en_GB&market=GB&client_id=2&channel_id=1&useCombined=true&from={start}&to={end}'

DETAIL_URL = 'https://eu-offering.kambicdn.org/offering/v2018/ub/betoffer/event/{id}.json?lang=en_GB&market=GB&client_id=2&channel_id=1&includeParticipants=true'

class UnibetSpider(scrapy.Spider):
    name = "unibet"
    custom_settings = {
        'DEFAULT_REQUEST_HEADERS':  {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
            'sec-ch-ua-mobile': '?0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Accept-Language': 'en-US,en;q=0.9,es;q=0.8,ca;q=0.7,gl;q=0.6',
            }
        }
    

    def start_requests(self) -> Request:
        date_time = datetime.now()
        start_time = date_time.strftime("%Y%m%dT%H%M%S%%2B0200")
        end_time = date_time + timedelta(hours=24)
        end_time = end_time.strftime("%Y%m%dT%H%M%S%%2B0200")

        yield Request(URL.format(start = start_time , end = end_time))

    def parse(self, response: Response) -> Request:
        json_response = json.loads(response.text)
        for events in json_response['events']:
            id=events['event']['id']
            yield Request(DETAIL_URL.format(id=id), callback=self.parse_detail)
    
    def parse_detail(self, response: Response) -> UnibetItem:
        json_response = json.loads(response.text)
        betData = json_response['betOffers']
        matchData = json_response['events'][0]

        for id, bet in self.extract_bets(betData).items():
            item = UnibetItem()
            item['matchName'] = matchData['name']
            item['matchId'] = matchData['id']

            item['participant1'] = matchData['homeName']
            item['participant2'] = matchData['awayName']

            item['startDate'] = matchData['start']

            item['sport'] = matchData['sport']
            item['stage'] = matchData['state']
            item['competition'] = matchData['group']
            item['region'] = matchData['path'][1]['englishName']
            item['betId'] = id

            item['rawData'] = bet
            yield item
        
       
    def extract_bets(self ,betData: list) -> dict:
        bet_dict = {}
    
        for bet in betData:
            outcomes = [x for x in bet['outcomes'] if 'odds' in x.keys()]
            if len(outcomes) != 2:
                continue
            bet_dict[bet['id']] = bet

        return bet_dict
