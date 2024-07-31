import os
from dotenv import load_dotenv
from auxiliar_generators.user_generate import UserGenerate
load_dotenv()

URI = os.getenv('URI')

UserGenerate.URI = URI
UserGenerate.user_generation()