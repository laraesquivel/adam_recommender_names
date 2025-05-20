from pymongo import MongoClient

# RECOMENDAÇAO DE FRASES (RODAR SEMPRE PARA NOVOS USÁRIOS CRIADOS)

class Phrases_Recommender_System:
    URI =''
    def __init__(self) -> None:
        pass
    
    @classmethod
    def generate_recommeder(cls):
        client = MongoClient(cls.URI)
        babynames = client['babynames']
        users = babynames['users']
        phrases = babynames['phrases']
        print('Connected to MongoDB')
        
        try:
            client.admin.command('ping')  # Testa a conexão
            print("Conectado ao MongoDB!")

            for user in users.find({}):  # Tenta executar a consulta
                print(user)

        except Exception as e:
            print(f"Erro: {e}")


        
        for user in users.find({"userId" : {'$exists' : True}, "assignature" : {'$exists' : True}, "phrases" : {'$exists' : False}}):
            print(user)
            # Get all phrases for this user
            this_user_phrases = []
            for phrase in phrases.find({}):
                if phrase['assignature'] == user['assignature']:
                    this_user_phrases.append(phrase)

            print(this_user_phrases)
            users.update_one({'_id':user['_id']},
                             {'$set' : {'phrases' : this_user_phrases}})
    @classmethod
    def set_uri(cls,URI):
        cls.URI = URI


Phrases_Recommender_System.set_uri('mongodb+srv://laraesquivel:OVyyiX5pIMj4vthh@babys.iuiuuvp.mongodb.net/')
Phrases_Recommender_System.generate_recommeder()