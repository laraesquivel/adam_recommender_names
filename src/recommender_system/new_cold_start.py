from pymongo import MongoClient
import pandas as pd
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
        new_names = babynames_db['newNames']

        # Carregando o arquivo com os nomes mais populares por década
        popular_names = pd.read_csv('src/brazilian_names_processing/nomes_populares por_decada.csv')

        # Selecionando o array de nomes para refazer a busca fria
        names_doc = names_collection.find({}, {"name": 1, "recommendedNames": 1, "gender": 1})

        
        # Percorrendo as linhas da coleção para depois percorrer o array de nomes recomendados
        for line in names_doc:
            name = line.get('name') # nome principal
            recommended_names = line.get('recommendedNames', []) # array de nomes recomendados
            genero = line.get('gender') # gênero do nome principal

            # Criar um array para colocar somente os nomes recomendados que estão na coleção de nomes brasileiros
            recommended_names_in_brazilian_names = []

            for recomended_name in recommended_names:

                # Buscando o nome recomendado na coleção de nomes brasileiros
                brazilian_name_doc = brazilian_names_collection.find_one({"nome_x": recomended_name})

                # Se o nome for encontrado, ele é adicionado na nova recomendação da busca fria
                if brazilian_name_doc is not None:
                    
                    # Verificando se o gênero do nome recomendado é igual ao gênero do nome principal
                    if brazilian_name_doc.get('gender') == genero or genero == 'U':
                        recommended_names_in_brazilian_names.append(recomended_name)
            

            # Preencher as listas de nomes recomendados que faltam nomes com a lista de nomes mais populares
            int = 0 # Variável para controlar a década de onde será escolhido o nome
            if genero == 'F': # Se o gênero do nome principal for feminino
                # Enquanto a lista de nomes recomendados não tiver 10 nomes
                while len(recommended_names_in_brazilian_names) < 10:
                    if int == 0:
                        random_name_F = random.choice(popular_names['FEMININOS 60'].values)
                    elif int == 1:
                        random_name_F = random.choice(popular_names['FEMININOS 70'].values)
                    elif int == 2:
                        random_name_F = random.choice(popular_names['FEMININOS 80'].values)
                    elif int == 3:
                        random_name_F = random.choice(popular_names['FEMININOS 90'].values)
                    elif int >= 4:
                        random_name_F = random.choice(popular_names['FEMININOS 2000'].values)
                    int += 1

                    # Adicionando o nome recomendado na lista de nomes recomendados
                    if random_name_F not in recommended_names_in_brazilian_names:
                        recommended_names_in_brazilian_names.append(random_name_F)
                    else:
                        continue # Se o nome já estiver na lista, ele é ignorado

            elif genero == 'M': # Se o gênero do nome principal for masculino
                    while len(recommended_names_in_brazilian_names) < 10:
                        if int == 0:
                            random_name_M = random.choice(popular_names['MASCULINOS 60'].values)
                        elif int == 1:
                            random_name_M = random.choice(popular_names['MASCULINOS 70'].values)
                        elif int == 2:
                            random_name_M = random.choice(popular_names['MASCULINOS 80'].values)
                        elif int == 3:
                            random_name_M = random.choice(popular_names['MASCULINOS 90'].values)
                        elif int >= 4:
                            random_name_M = random.choice(popular_names['MASCULINOS 2000'].values)
                        int += 1

                        if random_name_M not in recommended_names_in_brazilian_names:
                            recommended_names_in_brazilian_names.append(random_name_M)
                        else:
                            continue
            else: # Se o genero do nome principal for unissex
                while len(recommended_names_in_brazilian_names) < 10:
                    if int == 0:
                        random_name_M = random.choice(popular_names['MASCULINOS 60'].values)
                    elif int == 1:
                        random_name_M = random.choice(popular_names['FEMININOS 70'].values)
                    elif int == 2:
                        random_name_M = random.choice(popular_names['MASCULINOS 80'].values)
                    elif int == 3:
                        random_name_M = random.choice(popular_names['FEMININOS 90'].values)
                    elif int >= 4:
                        random_name_M = random.choice(popular_names['MASCULINOS 2000'].values)
                    int += 1

                    if random_name_M not in recommended_names_in_brazilian_names:
                        recommended_names_in_brazilian_names.append(random_name_M)
                    else:
                        continue
            
            # Colocar a lista de nomes recomendados na nova tabela de nomes
            selected_name = new_names.find_one({"name": name})
            selected_name['recommendedNames'] = recommended_names_in_brazilian_names  

            # Salvar os dados da nova busca fria em um arquivo JSON
            with open('new_cold_start.json', 'a', encoding="utf-8") as file:
                json.dump({name: recommended_names_in_brazilian_names}, file, ensure_ascii=False, indent=4)
                file.write('\n')

        # Fecha a conexão e o arquivo
        client.close()
        file.close()

    @classmethod
    def brazilian_names_cold_start(cls):
        client = MongoClient(cls.URI)
        babynames_db = client['babynames']

        names = babynames_db['brazilianNames']
        new_names = babynames_db['newNames']

        # Obtendo todos os nomes da tabela 'brazilianNames' como uma lista
        all_names = [doc['name'] for doc in names.find({}, {"name": 1, "_id": 0})]

        names_withou_recs = names.find({
                "recommendedNames": { "$exists": True, "$not": { "$size": 10 } }
                })
        
        for registro in names_withou_recs:
            print(registro['name'])

            # Preenchendo a lista de nomes recomendados com os nomes brasileiros aleatórios da coleção
            while len(registro['recommendedNames']) < 10:
                random_name = random.choice(all_names)
                if random_name not in registro['recommendedNames']:
                    registro['recommendedNames'].append(random_name)
                else:
                    continue
                
            # Atualize o documento no banco com os nomes recomendados
            names.update_one(
                {"_id": registro['_id']},
                {"$set": {"recommendedNames": registro['recommendedNames']}}
            )

            # Salvar os dados da nova busca fria em um arquivo JSON
            with open('new_cold_start.json', 'a', encoding="utf-8") as file:
                json.dump({registro['name']: registro['recommendedNames']}, file, ensure_ascii=False, indent=4)
                file.write('\n')

        # Fecha a conexão e o arquivo
        client.close()
        file.close()        
        

NewColdStart.set_URI('mongodb+srv://laraesquivel:OVyyiX5pIMj4vthh@babys.iuiuuvp.mongodb.net/')
NewColdStart.process_collections()
NewColdStart.brazilian_names_cold_start()