from pymongo import MongoClient
import random
import json


class ColdStartPopularity:
    URI = ""
    TOP100 = None
    DATA = None
    names_and_recommendations ={}

    @classmethod
    def set_URI(cls,URI):
        cls.URI = URI
        
    @classmethod
    def top_100(cls):
        client = MongoClient(cls.URI)
        babynames_db = client['babynames']
        names = babynames_db['names']

        top = {}

        for registro in names.find({}):
            recs = registro['recommendedNames']
            for name in recs:
                if name not in top:
                    top[name] = 0
                top[name] = top[name] + 1
        del top[None]
        maiores = sorted(top.keys(), key=lambda x:top[x],reverse=True)

        cls.TOP100 = maiores[:100]

        client.close()

    @classmethod
    def cold_start_popularity(cls):
        client = MongoClient(cls.URI)
        babynames_db = client['babynames']
        names = babynames_db['names']

        names_withou_recs = names.find({
                "recommendedNames": { "$exists": True, "$not": { "$size": 10 } }
                })
        
        for registro in names_withou_recs:
            name = registro['name']

            cls.names_and_recommendations[name] = set(registro['similiarNames']) if registro['similiarNames'] else set({})
            cls.names_and_recommendations[name].discard(name)
            size = len(cls.names_and_recommendations[name]) if cls.names_and_recommendations is not None else 0

            if size < 10:
                random_names = set(random.sample(cls.TOP100, 10 - size))
                cls.names_and_recommendations[name] = cls.names_and_recommendations[name] | random_names

            cls.names_and_recommendations[name] = list(cls.names_and_recommendations[name])[:10]
        
        with open('names_without_cold_start', 'w') as file:
            json.dump(cls.names_and_recommendations, file, indent=4)

    @classmethod
    def read(cls,file_path):
        with open(file_path, 'r') as file:
            cls.DATA = json.load(file)
    
    @classmethod
    def batch_insert(cls):
        client = MongoClient(cls.URI)
        dev = client['dev']
        names = dev.get_collection('dev_names')
        for chave, valor in cls.DATA.items():
            filter_n = {'name' : chave}
            names.update_one(filter_n,{
                "$set" : {"recommendedNames" : valor}
            } )
        
        client.close()




ColdStartPopularity.set_URI('mongodb+srv://laraesquivel:OVyyiX5pIMj4vthh@babys.iuiuuvp.mongodb.net/')

ColdStartPopularity.read('./src/files/names_without_cold_start')

ColdStartPopularity.batch_insert()