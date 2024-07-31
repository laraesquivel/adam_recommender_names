from pymongo import MongoClient
import requests
from geopy.geocoders import Nominatim

class LocationGenerate:
    URI = ''
    NOMITATION = Nominatim(user_agent='Hera_ADAM_UEFS')
    def __init__(self) -> None:
        pass

    @classmethod
    def set_URI(cls, URI):
        cls.URI = URI
    
    @classmethod
    def get_locations_for_all_actions(cls):
        client = MongoClient(cls.URI)
        babynames_actions = client['babynames']['actions']
        babynames_location = client['babynames']['location']

        pipeline_find_no_none = {
            "location" : {'$eq' : None},
            "lat" : {'$ne' : None},
            "lon" : {'$ne' : None}
        }

        for action in babynames_actions.find(pipeline_find_no_none):

            action_lat, action_lon = action['lat'], action['lon']
            location = None
            location_id = None

            if action_lon and action_lat:
                location = cls.NOMITATION.reverse((action_lat, action_lon), addressdetails=True)

            if location:
                address = location.raw['address']
                if address:
                    locs = {'city' : address.get('city',''),
                            'country' : address.get('country', ''),
                            'state' : address.get('state', ''),
                            'region' : address.get('region', '')}
                    
                    results = babynames_location.find_one(locs)

                    if not results:
                        locs['coords'] = {'type' : 'Point', 'coordinates' : [(action['lon'], action['lat'])]}
                        result_insert = babynames_location.insert_one(locs)
                        location_id = result_insert.inserted_id

                    else:
                        location_id = results['_id']
                        babynames_location.update_one(
                            {'_id' : results['_id']},
                            {'$push': {'coords.coordinates': [action['lon'], action['lat']]}},
                        
                        )
                babynames_actions.update_one(
                    {'_id' : action['_id']},
                    {'$set':{
                        'location' : location_id
                    }}
                )

            else:
                print(f'Error!')





LocationGenerate.set_URI('mongodb+srv://laraesquivel:OVyyiX5pIMj4vthh@babys.iuiuuvp.mongodb.net/')
LocationGenerate.get_locations_for_all_actions()

    

