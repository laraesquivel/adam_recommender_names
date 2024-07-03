import json
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pprint import pprint
from itertools import groupby


class ImplicitCollaborativeRecommenderSystem:
    URI = ''
    names_to_update = {}
    def __init__(self,URI) -> None:
        ImplicitCollaborativeRecommenderSystem.URI = URI 

    @classmethod
    def set_URI(cls, URI) -> None:
        cls.URI = URI

    @classmethod
    def implict_collaborative_recommender_system(cls):
        db = MongoClient(cls.URI)
        actions_db = db.get_database('dev').get_collection('dev_actions')
        actions = actions_db.find({'relationalName' : {'$exists' : True}})

        names_list = set({}) #nomes a sofrerem alteracoes 
        for action in actions:
            names_list.add(action['name'])
            names_list.add(action['relationalName'])
        

        for name in names_list:
            this_name_actions = list(actions_db.find({'$or' : [{'name' : name}, {'relationalName' : name}], 'userId' : {'$ne' : None}}))#todas as interações com o nome n

            usuarios = {} # usuario : peso
            this_name_actions.sort(key=lambda doc:doc['userId'])
            grouped_actionsby_users_for_this_name = groupby(this_name_actions, key=lambda doc : doc['userId'])

            for key, docs in grouped_actionsby_users_for_this_name: #Numero de interacoes do usuario u com o nome n
                usuarios[key] = 0
                for doc in docs:
                    usuarios[key] +=1
            
            usuarios = sorted(usuarios.items(), key=lambda item : item[1], reverse=True)[0:30]
            names = {}
            for user, peso in usuarios: #Obs: Considerar um único nome, ou mais de um nome?
                actions_of_user_u = actions_db.find({'userId' : user, 'relationalName' : {'$exists' : True}})
                for actions_of_u in actions_of_user_u:
                    n_action = actions_of_u['name']
                    nr_action = actions_of_u['relationalName']
                    if n_action not in names:
                        names[n_action] = 0
                    if nr_action not in names:
                        names[nr_action] = 0
                    names[n_action] += peso
                    names[nr_action] +=peso

            cls.names_to_update[name] = names
    
    @classmethod
    def update_recs(cls):
        db = MongoClient(cls.URI)
        names_db = db.get_database('dev').get_collection('dev_names')


        for name_to_up, recs_list in cls.names_to_update.items():
            reg = names_db.find_one({'name': name_to_up})
            if reg:
                actual_recs = reg['recommendedNames']
                recs_list.pop(name_to_up, None)
                recs_list.pop(None, None)
                sorted_names = [item[0] for item in sorted(recs_list.items(), key= lambda item : item[1], reverse=True)]
                #sorted_names2 = set(sorted_names)
                for old_name in actual_recs:
                    if not old_name in sorted_names:
                        sorted_names.append(old_name)
                if name_to_up in sorted_names:
                    sorted_names.remove(name_to_up)

                sorted_names =  sorted_names[:10]
                names_db.update_one({'name' : name_to_up}, {'$set' : {'recommendedNames' : sorted_names}})


#a = ImplicitCollaborativeRecommenderSystem()
#a.implict_collaborative_recommender_system()
#a.update_recs()

