import os
from dotenv import load_dotenv
from implicit_collaborative_recommender_systyem import ImplicitCollaborativeRecommenderSystem

load_dotenv()

URI = os.getenv('URI')

ImplicitCollaborativeRecommenderSystem.set_URI(URI)
print('recs!')
ImplicitCollaborativeRecommenderSystem.implict_collaborative_recommender_system()
print('colaboragive finish')
ImplicitCollaborativeRecommenderSystem.update_recs()
print('finish!')