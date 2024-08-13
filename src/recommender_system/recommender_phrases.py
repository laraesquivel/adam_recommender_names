from pymongo import MongoClient

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

        
        for user in users.find({}):
            this_user_phrases = []
            for phrase in phrases.find({}):
                if phrase['assignature'] == user['assignature']:
                    this_user_phrases.append(phrase)

            print(this_user_phrases)


URI = 'mongodb+srv://laraesquivel:OVyyiX5pIMj4vthh@babys.iuiuuvp.mongodb.net/'
Phrases_Recommender_System.URI = URI
Phrases_Recommender_System.generate_recommeder()

