from pymongo import MongoClient
import random

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

        # For all users with userId and assignature, get all phrases with matching bits and add them to the user
        for user in users.find({"userId" : {'$exists' : True}, "assignature" : {'$exists' : True}}):
            print(user)
            print("\n")
            # Get all phrases for this user based on bitwise comparison
            this_user_phrases = []
            user_signature = user['assignature']
            
            # Convert binary string to integer for bitwise operations
            if isinstance(user_signature, str):
                user_bits = int(user_signature, 2)
            else:
                user_bits = user_signature
            
            for phrase in phrases.find({"assignature" : {'$exists' : True}}):
                phrase_signature = phrase['assignature']
                
                # Convert binary string to integer for bitwise operations
                if isinstance(phrase_signature, str):
                    phrase_bits = int(phrase_signature, 2)
                else:
                    phrase_bits = phrase_signature
                
                # Check if all bits set to 1 in the phrase are also set to 1 in the user
                # This ensures the user has all characteristics required by the phrase
                if (user_bits & phrase_bits) == phrase_bits:
                    this_user_phrases.append(phrase)

            # Selecte randomly 10 phrases from this_user_phrases
            if len(this_user_phrases) > 10:
                this_user_phrases = random.sample(this_user_phrases, 10)

            print(this_user_phrases)
            users.update_one({'_id':user['_id']},
                             {'$set' : {'phrases' : this_user_phrases}})
    @classmethod
    def set_uri(cls,URI):
        cls.URI = URI


Phrases_Recommender_System.set_uri('mongodb+srv://laraesquivel:OVyyiX5pIMj4vthh@babys.iuiuuvp.mongodb.net/')
Phrases_Recommender_System.generate_recommeder()