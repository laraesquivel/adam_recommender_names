from pymongo import MongoClient
import random
import json
import requests

class MixingNamesTables:
    URI = ""

    @classmethod
    def set_URI(cls,URI):
        cls.URI = URI

    @classmethod
    def process_collections(cls):

        # Conectando com o MongoDB
        client = MongoClient(cls.URI)
        babynames_db = client['babynames']

        # Conecção com as coleções para serem processadas
        names_collection = babynames_db['names']
        brazilian_names_collection = babynames_db['brazilianNames']

        # Adicionando o campo 'recommendedNames' na coleção de nomes brasileiros
        brazilian_names_collection.update_many({}, {"$set": {"recommendedNames": []}})

        # Adicionando o campo 'quantity_births_until_2010' na coleção de nomes
        names_collection.update_many({}, {"$set": {"quantity_births_until_2010": 0}})

        # Adicionando o campo 'brazilian_region' na coleção de nomes
        names_collection.update_many({}, {"$set": {"brazilian_region": ""}})

        # Renomeando o campo 'genero' em todos os documentos da tabela de nomes brasileiros
        resultado = brazilian_names_collection.update_many(
            {},  # Filtro vazio para aplicar a todos os documentos
            {"$rename": {"genero": "gender"}}  # Substitua pelos nomes do campo
        )

        # Percorrendo a tabela de nomes brasileiros para mudar o nome dos campos 'gender' de 'feminino' para 'F' e 'masculino' para 'M'
        for doc in brazilian_names_collection.find({}, {"gender": 1}):
            if doc.get('gender') == 'feminino':
                brazilian_names_collection.update_one({"_id": doc.get('_id')}, {"$set": {"gender": "F"}})
            elif doc.get('gender') == 'masculino':
                brazilian_names_collection.update_one({"_id": doc.get('_id')}, {"$set": {"gender": "M"}})
            else:
                brazilian_names_collection.update_one({"_id": doc.get('_id')}, {"$set": {"gender": "U"}})

        # Fecha a conexão
        client.close()

    @classmethod
    def join_collections(cls):

        # Conectando com o MongoDB
        client = MongoClient(cls.URI)
        babynames_db = client['babynames']

        # Conecção com as coleções
        names_collection = babynames_db['names']
        brazilian_names_collection = babynames_db['brazilianNames']

        # Criando a nova coleção para a junção das duas outras
        try:
            babynames_db.create_collection("newNames")
            #print("Coleção criada com sucesso!")
        except Exception as e:
            print(f"Erro ao criar coleção: {e}")

        new_names = babynames_db['newNames']

        # Adicionando os nomes (documentos) na nova coleção

        try:
            start_name = "Rosemeiry"
            # Percorrendo toda a tabela de nomes antigos, verificando se o nome existe na tabela de nomes brasileiros e mesclando as informações das duas tabelas
            for doc in names_collection.find({"name": {"$gte": start_name}}):
                    name = doc.get('name')
                    print(name)
                    brazilian_name_doc = brazilian_names_collection.find_one({"nome_x": name})

                    if brazilian_name_doc is not None:
                        new_names.insert_one({
                            "name": name,
                            "searchCount": doc.get('searchCount'),
                            "femaleCount": doc.get('femaleCount'),
                            "maleCount": doc.get('maleCount'),
                            "origin": doc.get('origin'),
                            "meaning": doc.get('meaning'),
                            "brazilian_region": brazilian_name_doc.get('nome_regiao'),
                            "gender": brazilian_name_doc.get('gender'),
                            "quantity_births_until_2010": brazilian_name_doc.get('quantidade_nascimentos_ate_2010'),
                            "recommendedNames": [],
                        })
                    else:
                        new_names.insert_one({
                            "name": name,
                            "searchCount": doc.get('searchCount'),
                            "femaleCount": doc.get('femaleCount'),
                            "maleCount": doc.get('maleCount'),
                            "origin": doc.get('origin'),
                            "meaning": doc.get('meaning'),
                            "brazilian_region": doc.get('brazilian_region'),
                            "gender": doc.get('gender'),
                            "quantity_births_until_2010": doc.get('quantity_births_until_2010'),
                            "recommendedNames": [],
                        })

            # Perorrendo a tabela de nomes brasileiros para acrescentar os nomes que não foram adicionados por não estarem na tabela de nomes antigos
            print("Nomes brasileiros")
            for br_doc in brazilian_names_collection.find():
                    br_name = br_doc.get('nome_x')
                    print(br_name)
                    name_doc = names_collection.find_one({"name": br_name})

                    if name_doc is None:
                        new_names.insert_one({
                            "name": br_name,
                            "searchCount": 0,
                            "femaleCount": 0,
                            "maleCount": 0,
                            "origin": "",
                            "meaning": "",
                            "brazilian_region": br_doc.get('nome_regiao'),
                            "gender": br_doc.get('gender'),
                            "quantity_births_until_2010": br_doc.get('quantidade_nascimentos_ate_2010'),
                            "recommendedNames": [],
                        })
        except requests.exceptions.RequestException as e:
            print(f"Erro: {e}")

        # Feche a conexão
        client.close()

MixingNamesTables.set_URI('mongodb+srv://laraesquivel:OVyyiX5pIMj4vthh@babys.iuiuuvp.mongodb.net/')

#MixingNamesTables.process_collections()
#MixingNamesTables.join_collections() #outra função para só exexutar o comando de juntar as tabelas e não fazer tudo de novo