from pymongo import MongoClient
from collections import Counter
from datetime import datetime, timedelta
from pipelines import get_region, get_region_origin, get_origins
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

        begin_of_year = today - timedelta(today.year,1,1)
        end_of_year = datetime(today.year +1,1,1)
        results = None

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
                for palavra,classificado in cls.ALL_BRAZIL_REGION:
                    if classificado in frase:
                        for palavraO, classificadoO in frase:
                            if palavraO in frase:
                                results = actions.aggregate(get_region_origin.pipeline(classificado))
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

Phrases_State.workflow()