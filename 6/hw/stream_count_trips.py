import faust
from rides_fhv import FHVRide
from rides_green import GREENRide
from settings import BROKER, FAUST_APP, KAFKA_TOPIC_F, KAFKA_TOPIC_G


app = faust.App(FAUST_APP, broker=BROKER)

topic_fhv = app.topic(KAFKA_TOPIC_F, value_type=FHVRide)
topic_green = app.topic(KAFKA_TOPIC_G, value_type=GREENRide)

vendor_rides = app.Table('vendor_rides', default=int)

@app.agent(topic)
async def process(stream):
    async for event in stream:
        print(event)


if __name__ == '__main__':
    app.main()