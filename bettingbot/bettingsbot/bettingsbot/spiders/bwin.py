import scrapy
from scrapy.http import Request, Response
import json

from bettingsbot.items import BwinItem

DETAIL_URL = 'https://cds-api.bwin.es/bettingoffer/fixture-view?x-bwin-accessid=OTdhMjU3MWQtYzI5Yi00NWQ5LWFmOGEtNmFhOTJjMWVhNmRl&lang=en&country=GB&userCountry=GB&offerMapping=All&scoreboardMode=Full&fixtureIds={id}&state=Latest&includePrecreatedBetBuilder=true'

URL = 'https://cds-api.bwin.es/bettingoffer/fixtures?x-bwin-accessid=OTdhMjU3MWQtYzI5Yi00NWQ5LWFmOGEtNmFhOTJjMWVhNmRl&lang=en&country=GB&userCountry=GB&fixtureTypes=Standard&state=Latest&offerMapping=Filtered&offerCategories=Gridable&fixtureCategories=Gridable,NonGridable,Other,Specials,Outrights&sportIds=4&regionIds=&competitionIds=&skip=0&take=1000&sortBy=Tags'


class BwinSpider(scrapy.Spider):
    name = "bwin"

    def start_requests(self) -> Request:
            yield Request(URL)

    def parse(self, response: Response) -> Request:
        json_response = json.loads(response.text)
        for fixture in json_response['fixtures']:
            id = fixture['id']
            yield Request(DETAIL_URL.format(id=id), callback=self.parse_detail)
    
    def parse_detail(self, response: Response) -> BwinItem:
        json_response = json.loads(response.text)
        fixture = json_response['fixture']

        for id, bet in self.extract_bets(fixture).items():
            item = BwinItem()
            item['matchId'] = fixture['id']
            item['matchName'] = fixture['name']['value']

            item['startDate'] = fixture['startDate']
            item['cutOffDate'] = fixture['cutOffDate']

            item['region'] = fixture['region']['code']
            item['sport'] = fixture['sport']['name']['value']
            item['stage'] = fixture['stage']
            item['betId'] = id
            item['competition'] = fixture['competition']['name']['value']

            item['participant1'] = fixture['participants'][0]['name']['value']
            item['participant2'] = fixture['participants'][1]['name']['value']

            item['rawData'] = bet
            yield item
        
    
    def extract_bets(self, fixture: list) -> dict:
        bet_dict = {}

        for bet in fixture['optionMarkets']:
            if len(bet['options']) != 2:
                continue
            bet_dict[bet['id']] = bet
        
        return bet_dict