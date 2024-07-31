from pymongo import MongoClient


class UserGenerate:
    URI = ''
    ALL_ORIGIN = ["Alemã", "Árabe", "Espanhola", "Francesa", "Grega", "Hebraica", "Indígena", "Inglesa", "Japonesa", "Lusitana"]
    ALL_BRAZIL_REGION = ['Região Norte', 'Região Nordeste', 'Região Centro-Oeste', 'Região Sudeste', 'Região Sul']
    GENDER = ['M', 'F']

    def __init__(self) -> None:
        pass
    

    @classmethod
    def user_generation(cls):
        client = MongoClient(cls.URI)
        babynames = client['babynames']
        users = babynames['users']
        actions = babynames['actions']
        names = babynames['names']
        location = babynames['location']

        for user in users.find({}):
           # print(user)
            u = str(user['_id'])

            this_user_actions = actions.find({'userId' : u})
            G = {}
            R = {}
            O = {}
           # print(this_user_actions)
            
            for action in this_user_actions:
                names_reg = names.find_one({'name' : action['name'], 'origin' : {'$exists' : True}})
                o = None
                g = None
                if names_reg:
                    o = names_reg['origin']
                    g = names_reg['gender']
                if g:
                    if not g in G:
                        G[g] = 0
                        
                        G[g] = G[g] + 1

                location_reg = location.find_one({'_id' : action['location']})
                if location_reg:
                    if not location_reg['region'] in R:
                        R[location_reg['region']] =0
                    
                    R[location_reg['region']] = R[location_reg['region']] + 1
                if o:
                    if not o in O:
                        O[o] = 0
                    O[o] = O[o] + 1
            bins_list = cls.binarization(G, R,O)
            users.update_one({'_id':user['_id']},{'$set': {'preferences' : {
                'gender' : G,
                'region' : R,
                'origin' : O
            },
            'assignature' : bins_list} })


        
    @classmethod
    def binarization(cls,G, R, O):
        bins_lists = lambda lista: [(1 if x == max(lista) else 0) for x in lista]
        lista_para_string_binaria = lambda lista: ''.join(map(str, lista))


        origin_list = bins_lists([O.get(origem, 0) for origem in cls.ALL_ORIGIN]) #Origin ordenadas por ordem alfabetica
        

        gender_list = bins_lists([G.get(genero, 0) for genero in cls.GENDER])
        region_list = bins_lists([R.get(regiao,0) for regiao in cls.ALL_BRAZIL_REGION])

        binarized_list = gender_list + region_list + origin_list
        return lista_para_string_binaria(binarized_list)





                    




UserGenerate.user_generation()

    