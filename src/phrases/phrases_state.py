from pymongo import MongoClient
from collections import Counter
from datetime import datetime, timedelta
from .pipelines import get_region, get_region_origin, get_origins

class Phrases_State:
    URI = 'mongodb+srv://laraesquivel:OVyyiX5pIMj4vthh@babys.iuiuuvp.mongodb.net/'
    WORDS_ORIGIN = (
    ('lusitana', 'Lusitana'),
    ('francesa', 'Francesa'),
    ('inglesa', 'Inglesa'),
    ('hebraica', 'Hebraica'),
    ('indígena', 'Indígena'),
    ('árabe', 'Árabe'),
    ('japonesa', 'Japonesa'),
    ('germânica', 'Alemã'),  # A nacionalidade "germânica" foi associada com "Alemã".
    ('alemã', 'Alemã')
)
    ALL_BRAZIL_REGION = (('norte','Região Norte'), ('nordeste','Região Nordeste'), ('centro-oeste','Região Centro-Oeste'), ('sudeste','Região Sudeste'), ('sul','Região Sul'))

    @classmethod
    def workflow(cls):
        client = MongoClient(cls.URI)

        phrases = client['babynames']['phrases']
        actions = client['babynames']['actions']

        today = datetime.now()
        begin_of_day = datetime(today.year,today.month,today.day)
        end_of_day = begin_of_day + timedelta(days=1)

        begin_of_week = today - timedelta(days=today.weekday())
        end_of_week = begin_of_week + timedelta(days=7)

        begin_of_year = datetime(today.year, 1, 1)
        end_of_year = datetime(today.year + 1, 1, 1)
        results = None

        try:

            for doc in phrases.find({}):
                frase = doc['Frase'].lower()
                time = ('hoje' in frase or 'semana' in frase or 'ano' in frase)

                if 'região' in frase and 'origem' in frase and time:
                    for palavra,classificado in cls.ALL_BRAZIL_REGION:
                        if classificado in frase:
                            for palavraO, classificadoO in cls.WORDS_ORIGIN:
                                if palavraO in frase:
                                    if 'hoje' in frase:
                                        results = actions.aggregate(get_region_origin.pipeline(classificadoO,begin_of_day,end_of_day))
                                    elif 'semana' in frase:
                                        results = actions.aggregate(get_region_origin.pipeline(classificadoO,begin_of_week,end_of_week))
                                    else:
                                        results = actions.aggregate(get_region_origin.pipeline(classificadoO,begin_of_year,end_of_year))
                elif 'região' in frase and 'origem' in frase:
                    for palavra, classificado in cls.ALL_BRAZIL_REGION:
                        if classificado in frase:
                            for palavraO, classificadoO in cls.WORDS_ORIGIN:
                                if palavraO in frase:
                                    results = actions.aggregate(get_region_origin.pipeline(classificadoO))
                elif 'região' in frase and time:
                    for palavra, classificado in cls.ALL_BRAZIL_REGION:
                        if palavra in frase:
                            if 'hoje' in frase:
                                results = actions.aggregate(get_region.pipeline(classificado,begin_of_day,end_of_day))
                            elif 'semana' in frase:
                                results = actions.aggregate(get_region.pipeline(classificado,begin_of_week,end_of_week))
                            else:
                                results = actions.aggregate(get_region.pipeline(classificado,begin_of_day,end_of_day))
                elif 'origem' in frase and time:
                    for palavra,classificado in cls.WORDS_ORIGIN:
                        if palavra in frase:
                            if 'hoje' in frase:
                                results = actions.aggregate(get_origins.pipeline(classificado,begin_of_day,end_of_day))
                            elif 'semana' in frase:
                                results = actions.aggregate(get_origins.pipeline(classificado,begin_of_week,end_of_week))
                            else:
                                results = actions.aggregate(get_origins.pipeline(classificado,begin_of_year,end_of_year))
                elif 'região' in frase:
                    for palavra, classificado in cls.ALL_BRAZIL_REGION:
                        if palavra in frase:
                            results = actions.aggregate(get_region.pipeline(classificado))
                elif 'origem' in frase:
                    for palavra, classificado in cls.WORDS_ORIGIN:
                        if palavra in frase:
                            results = actions.aggregate(get_origins.pipeline(classificado))
                elif time:
                    if 'hoje' in frase:
                        results = actions.find({'timestamp' : {'$gte' : begin_of_day, '$lt' : end_of_day}})
                    elif 'semana' in frase:
                        results = actions.find({'timestamp' : {'$gte' : begin_of_week, '$lt' : end_of_week}})
                    else:
                        results = actions.find({'timestamp' : {'$gte' : begin_of_year, '$lt' : end_of_year}})
                else:
                    results = actions.find({})

                names = Counter()
                for action in results:
                    if 'name' in action:
                        names[action['name']]+=1
                    if 'relationalName' in action:
                        names[action['relationalName']] += 1
                top10 = [top_name[0] for top_name in names.most_common(10)]
                phrases.update_one(
                    {'Frase' : doc['Frase']},
                    {
                        '$set' : {
                            'associedNames' : top10
                        }
                    }
                )
        except Exception as e:
            print(f"Erro ao processar as frases: {e}")

    @classmethod
    def cold_start_phrases(cls):
        client = MongoClient(cls.URI)
        names = client['babynames']['newNames']
        phrases = client['babynames']['phrases']
        
        for doc in phrases.find({}):
            if doc.get('associedNames') == []:
                print(f"Frase: {doc['Frase']}\n")
                
                random_names = []
                
                if doc.get('assignature') == '00000000000000000':  # Frases não específicas
                    random_names = [d['name'] for d in names.aggregate([
                        {'$sample': {'size': 10}},
                        {'$project': {'name': 1, '_id': 0}}
                    ])]

                elif doc.get('assignature') == '10000000000000000':  # Nomes masculinos
                    random_names = [d['name'] for d in names.aggregate([
                        {'$match': {'gender': 'M'}},
                        {'$sample': {'size': 10}},
                        {'$project': {'name': 1, '_id': 0}}
                    ])]

                elif doc.get('assignature') == '01000000000000000':  # Nomes femininos
                    random_names = [d['name'] for d in names.aggregate([
                        {'$match': {'gender': 'F'}},
                        {'$sample': {'size': 10}},
                        {'$project': {'name': 1, '_id': 0}}
                    ])]

                elif doc.get('assignature') == '00100000000000000':  # Região Norte
                    random_names = [d['name'] for d in names.aggregate([
                        {'$match': {'brazilian_region': 'Norte'}},
                        {'$sample': {'size': 10}},
                        {'$project': {'name': 1, '_id': 0}}
                    ])]

                elif doc.get('assignature') == '00010000000000000':  # Região Nordeste
                    random_names = [d['name'] for d in names.aggregate([
                        {'$match': {'brazilian_region': 'Nordeste'}},
                        {'$sample': {'size': 10}},
                        {'$project': {'name': 1, '_id': 0}}
                    ])]

                elif doc.get('assignature') == '00001000000000000':  # Região Centro-Oeste
                    random_names = [d['name'] for d in names.aggregate([
                        {'$match': {'brazilian_region': 'Centro-Oeste'}},
                        {'$sample': {'size': 10}},
                        {'$project': {'name': 1, '_id': 0}}
                    ])]

                elif doc.get('assignature') == '00000100000000000':  # Região Sudeste
                    random_names = [d['name'] for d in names.aggregate([
                        {'$match': {'brazilian_region': 'Sudeste'}},
                        {'$sample': {'size': 10}},
                        {'$project': {'name': 1, '_id': 0}}
                    ])]

                elif doc.get('assignature') == '00000010000000000':  # Região Sul
                    random_names = [d['name'] for d in names.aggregate([
                        {'$match': {'brazilian_region': 'Sul'}},
                        {'$sample': {'size': 10}},
                        {'$project': {'name': 1, '_id': 0}}
                    ])]

                if random_names:
                    print(f"Nomes aleatórios: {random_names}\n")
                    phrases.update_one(
                        {'Frase': doc['Frase']},
                        {'$set': {'associedNames': random_names}}
                    )


#Phrases_State.cold_start_phrases()
Phrases_State.workflow()