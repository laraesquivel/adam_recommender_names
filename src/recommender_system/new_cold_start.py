from pymongo import MongoClient
import random
import json

class NewColdStart:
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

        # Selecionando o array de nomes para refazer a busca fria
        names_doc = names_collection.find({}, {"name": 1, "recommendedNames": 1})

        # Dicionário para guardar o nome e seus novos nomes recomendados para salvar em um arquivo JSON
        names_with_new_cold_start = {}

        # Percorrendo as linhas da coleção para depois percorrer o array de nomes recomendados
        for line in names_doc:
            name = line.get('name') # nome principal
            recommended_names = line.get('recommendedNames', []) # array de nomes recomendados

            #print(f"Name: {name}")

            # Criar um array para colocar somente os nomes recomendados que estão na coleção de nomes brasileiros
            recommended_names_in_brazilian_names = []

            for recomended_name in recommended_names:

                # Buscando o nome recomendado na coleção de nomes brasileiros
                brazilian_name_doc = brazilian_names_collection.find_one({"nome_x": recomended_name})

                # Se o nome for encontrado, ele é adicionado na nova recomendação da busca fria
                if brazilian_name_doc is not None:
                    # Verificando se o gênero do nome recomendado é igual ao gênero do nome principal
                    #if brazilian_name_doc.get('gender')[0] == line.get('gender'):
                    recommended_names_in_brazilian_names.append(recomended_name)
            
            #names_with_new_cold_start[name] = recommended_names_in_brazilian_names

            # Salvar os dados da nova busca fria em um arquivo JSON
            with open('new_cold_start.json', 'a') as file:
                json.dump({name: recommended_names_in_brazilian_names}, file)
                file.write('\n')

        # Fecha a conexão
        client.close()
                    


NewColdStart.set_URI('mongodb+srv://laraesquivel:OVyyiX5pIMj4vthh@babys.iuiuuvp.mongodb.net/')
NewColdStart.process_collections()