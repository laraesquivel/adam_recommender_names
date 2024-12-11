from pymongo import MongoClient
import random
import json

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
        for doc in names_collection:
            name = doc.get('name')
            brazilian_name_doc = brazilian_names_collection.find_one({"nome_x": name})

            if brazilian_name_doc is not None:
                new_names.insert_one({
                    "name": name,
                    "searchCount": 
                    "femaleCount": 
                    "maleCount": 
                    "origin": 
                    "meaning": 
                    "brazilian_region": 
                    "gender": 
                    "quantity_births_until_2010": 
                    "recommendedNames": 
                })

        # Feche a conexão
        client.close()
    
        # Juntando as duas tabelas de nomes brasileiros e nomes em uma nova coleção tirando os nomes repetidos deixando os da tabela de nomes brasileiros
        # names_collection.aggregate([
        #     {"$lookup": {
        #         "from": "brazilianNames",
        #         "localField": "name",
        #         "foreignField": "nome_x",
        #         "as": "names"
        #     }},
        #     {"$unwind": "$names"},
        #     {"$out": "new_names"}
        # ])

        # Fecha a conexão
        client.close()


MixingNamesTables.set_URI('mongodb+srv://laraesquivel:OVyyiX5pIMj4vthh@babys.iuiuuvp.mongodb.net/')

#MixingNamesTables.process_collections()
MixingNamesTables.join_collections() #outra função para só exexutar o comando de juntar as tabelas e não fazer tudo de novo