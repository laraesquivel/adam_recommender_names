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
        actions_db = db.get_database('babynames').get_collection('actions')
        actions = actions_db.find({'relationalName' : {'$exists' : True}})
        
        names_db = db.get_database('babynames').get_collection('newNames')

        names_list = set({}) #nomes a sofrerem alteracoes 
        for action in actions:
            names_list.add(action['name'])
            names_list.add(action['relationalName'])
        
        #Percorre a lista dos nomes
        for name in names_list:
            this_name_actions = list(actions_db.find({'$or' : [{'name' : name}, {'relationalName' : name}], 'userId' : {'$ne' : None}}))#todas as interações com o nome n
            
            # Buscar o gênero do nome principal
            name_data = names_db.find_one({'name': name})
            main_gender = name_data['gender'] if name_data else None
            print(main_gender)
            #print(main_gender)
        
            usuarios = {} # usuario : peso
            this_name_actions.sort(key=lambda doc:doc['userId']) #ordena
            grouped_actionsby_users_for_this_name = groupby(this_name_actions, key=lambda doc : doc['userId'])

            for key, docs in grouped_actionsby_users_for_this_name: #Numero de interacoes do usuario u com o nome n
                usuarios[key] = 0
                for doc in docs:
                    usuarios[key] +=1
            
            usuarios = sorted(usuarios.items(), key=lambda item : item[1], reverse=True)[0:30] #Ordena do maior para o menor e pega os 30 primeiros
            names = {}
            for user, peso in usuarios: #Obs: Considerar um único nome, ou mais de um nome?
                actions_of_user_u = actions_db.find({'userId' : user, 'relationalName' : {'$exists' : True}})
                
                for actions_of_u in actions_of_user_u:
                    n_action = actions_of_u['name']
                    nr_action = actions_of_u['relationalName']

                    # Buscar os gêneros dos nomes recomendados
                    n_action_data = names_db.find_one({'name': n_action})
                    nr_action_data = names_db.find_one({'name': nr_action})

                    n_action_gender = n_action_data['gender'] if n_action_data else None
                    nr_action_gender = nr_action_data['gender'] if nr_action_data else None

                    # Filtrar os nomes que têm o mesmo gênero ou são unissex
                    if main_gender and main_gender != "U":
                        if n_action_gender == main_gender or n_action_gender == "U": #Se o nome não estiver no dicionário, adiciona
                            if n_action not in names:
                                names[n_action] = 0
                            names[n_action] += peso
                        if nr_action_gender == main_gender or nr_action_gender == "U":
                            if nr_action not in names:
                                names[nr_action] = 0
                            names[nr_action] += peso
                    else:                        
                        if n_action not in names: #Se o nome não estiver no dicionário, adiciona
                            names[n_action] = 0
                        if nr_action not in names:
                            names[nr_action] = 0
                        names[n_action] += peso
                        names[nr_action] +=peso
                        
                        
            cls.names_to_update[name] = names
            print(names)
        print(cls.names_to_update)
    
    @classmethod
    def update_recs(cls):
        db = MongoClient(cls.URI)
        names_db = db.get_database('babynames').get_collection('newNames')


        for name_to_up, recs_list in cls.names_to_update.items():
            reg = names_db.find_one({'name': name_to_up})
            if reg:
                actual_recs = reg['recommendedNames']
                recs_list.pop(name_to_up, None)
                recs_list.pop(None, None)
                sorted_names = {item[0] for item in sorted(recs_list.items(), key=lambda item : item[1], reverse=True)}
                
                # Adicionar recomendações antigas sem criar duplicatas
                sorted_names.update(actual_recs)

                # Remover novamente o próprio nome, caso tenha entrado na atualização
                sorted_names.discard(name_to_up)

                sorted_names = list(sorted_names)[:10]
                print(name_to_up)
                print(sorted_names)
                names_db.update_one({'name' : name_to_up}, {'$set' : {'recommendedNames' : sorted_names}})


a = ImplicitCollaborativeRecommenderSystem('mongodb+srv://laraesquivel:OVyyiX5pIMj4vthh@babys.iuiuuvp.mongodb.net/')
a.implict_collaborative_recommender_system()
a.update_recs()

