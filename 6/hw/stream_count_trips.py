import faust
from rides_temp import TaxiRide
from rides_fhv import FHVRide
from rides_green import GREENRide
from settings import BROKER, FAUST_APP, KAFKA_TOPIC_F, KAFKA_TOPIC_G


app = faust.App(FAUST_APP, broker=BROKER)

# topic_fhv = app.topic(KAFKA_TOPIC_F, value_type=FHVRide)
# topic_green = app.topic(KAFKA_TOPIC_G, value_type=GREENRide)
topic = app.topic(KAFKA_TOPIC_F, KAFKA_TOPIC_G, value_type=TaxiRide)

vendor_rides = app.Table('PickUp_Loc_count', default=int)

@app.agent(topic)
async def process(stream):
    async for event in stream.group_by(TaxiRide.pu_location_id):
        vendor_rides[event.pu_location_id] += 1


if __name__ == '__main__':
    app.main()