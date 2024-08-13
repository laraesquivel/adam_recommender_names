import os
from dotenv import load_dotenv
from recommender_system import recommender_phrases

load_dotenv()

URI = os.getenv('URI')

recommender_phrases.Phrases_Recommender_System.set_uri(URI)
recommender_phrases.Phrases_Recommender_System.generate_recommeder()