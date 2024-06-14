import os
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv('URI')

print('Ola mundo!')