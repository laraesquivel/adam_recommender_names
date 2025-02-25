from pymongo import MongoClient
import pandas as pd

class MaleFemaleNames:
    URI = ""

    @classmethod
    def set_URI(cls,URI):
        cls.URI = URI

    @classmethod
    def processing(cls):
        
        # Conectando com o MongoDB
        client = MongoClient(cls.URI)
        babynames_db = client['babynames']

        # Conecção com as coleções para serem processadas
        new_names = babynames_db['newNames']

        # Criando 2 arquivos .txt, um com todos os nomes masculinos e outro com todos os nomes femininos
        male_names_list = []
        female_names_list = []

        try:
            for doc in new_names.find():
                print(doc['name'])
                if doc['gender'] == 'M':
                    male_names_list.append(doc['name'])
                elif doc['gender'] == 'F':
                    female_names_list.append(doc['name'])
        except Exception as e:
            print(f"Erro ao processar os nomes: {e}")

        # Criando o arquivo .txt para os nomes masculinos
        with open("src/extra/nomes_masculinos.txt", "w", encoding="utf-8") as file_male:
            for nome in male_names_list:
                file_male.write(f"{nome}\n")

        # Criando o arquivo .txt para os nomes femininos
        with open("src/extra/nomes_femininos.txt", "w", encoding="utf-8") as file_female:
            for nome in female_names_list:
                file_female.write(f"{nome}\n")

        # Fechando a conexão com o MongoDB
        client.close()    


MaleFemaleNames.set_URI('mongodb+srv://laraesquivel:OVyyiX5pIMj4vthh@babys.iuiuuvp.mongodb.net/')
MaleFemaleNames.processing()
