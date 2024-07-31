import os
from dotenv import load_dotenv
from auxiliar_generators.location_genrate import LocationGenerate

load_dotenv()

URI = os.getenv('URI')

LocationGenerate.set_URI(URI)
LocationGenerate.get_locations_for_all_actions()