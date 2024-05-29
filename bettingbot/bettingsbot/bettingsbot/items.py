from scrapy.item import Item, Field

class BwinItem(Item):
    matchName = Field()
    matchId = Field()

    participant1 = Field()
    participant2 = Field()

    startDate = Field()
    cutOffDate = Field()

    sport = Field()
    stage = Field()
    competition = Field()
    region = Field()
    betId = Field()

    rawData = Field()

class UnibetItem(Item):
    matchName = Field()
    matchId = Field()

    participant1 = Field()
    participant2 = Field()

    startDate = Field()

    sport = Field()
    stage = Field()
    competition = Field()
    region = Field()
    betId = Field()

    rawData = Field()

class BetwayItem(Item):
    matchName = Field()
    matchId = Field()
