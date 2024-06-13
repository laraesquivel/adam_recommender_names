import json
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pprint import pprint
from itertools import groupby

class BasicRecommenderSystem:
    URI = ""
    def __init__(self, URI) -> None:
        BasicRecommenderSystem.URI = URI
        self.__current_dir = Path(__file__).parent
        self.__PIPELINE_NAMES_FILTERING = None
        self.__next_dir = f'{self.__current_dir}/pipelines/names_filtering.json'
        with open(self.__next_dir, 'r') as file:
            self.__PIPELINE_NAMES_FILTERING = json.load(file)
        
    def names_filtering(self):
        try:
            client = MongoClient(BasicRecommenderSystem.URI)
            client.admin.command('ping')

            
            db = client['babynames']
            
            collection_actions = db.actions
            
            if self.__PIPELINE_NAMES_FILTERING:
                result = collection_actions.aggregate(self.__PIPELINE_NAMES_FILTERING)
                for doc in result:
                    print(doc)
                    print("\n")
                return result
            
            raise NotImplementedError
        
        except ConnectionFailure:

            print("Falha na conexão com o MongoDB")

    def get_all_names_relations(self):
        client = MongoClient(BasicRecommenderSystem.URI)
        client.admin.command('ping')

        
        db = client['babynames']
        
        collection_actions = list(db.actions.find().sort('relationalName',1))

        collection_actions_grouped = {key if key is not None else 'SearchEmpty': list(group) for key, group in groupby(collection_actions, key=lambda doc: doc.get('relationalName', None))}
        return collection_actions_grouped
    
    def names_filtering_user(self):
        names_weight = []  #N
        #all_names = self.names_filtering()
        names_relations = self.get_all_names_relations()
        conjunto_popularity = {}
        for chave, objeto_acao in names_relations.items():
            conjunto_popularity[chave] = {}
            for objeto in objeto_acao:
                if objeto['name'] not in  conjunto_popularity[chave]:
                    conjunto_popularity[chave][objeto['name']] = 0
                
                conjunto_popularity[chave][objeto['name']] = conjunto_popularity[chave][objeto['name']] + 1
        
        return conjunto_popularity

    def get_actual_recs(self):
        conjunto_popularity = self.names_filtering_user()
        client = MongoClient(BasicRecommenderSystem.URI)
        pprint(conjunto_popularity)
        for chave, objeto in conjunto_popularity.items():
            item_collection = client['babynames'].names.find_one({"name" : "Lara"})
            recommended_names = item_collection.get('recommendedNames', [])
            aux_objt  = dict(sorted(objeto.items(), key= lambda item : item[1]))
            for nome_a_inserir_na_rec, frequencia_nome in aux_objt.items():
                recommended_names.insert(0, nome_a_inserir_na_rec)
            
            recommended_names = set(recommended_names)
            recommended_names.discard(chave)
            recommended_names = list(recommended_names)
            
            client['babynames'].names.update_one({'name' : chave} ,{'$set': {'recommendedNames': recommended_names[0:10]}})
    
#c = BasicRecommenderSystem()
#c.get_actual_recs()