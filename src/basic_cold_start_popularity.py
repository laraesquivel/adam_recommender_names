from pymongo import MongoClient
import random


class BasicColdStartPopularity:
    URI = ""

    def __init__(self, URI) -> None:
        BasicColdStartPopularity.URI = URI

    @classmethod
    def random_popularity(cls):
        client = MongoClient(cls.URI)

        babynames_names = client['dev']['names']
        documents = babynames_names.find()
        filtered_documents = [doc for doc in documents if len(doc.get('recommendedNames', [])) < 10]
        print(len(filtered_documents))
        for document in filtered_documents:
            random_indices = random.sample(range(filtered_documents.count({}) -1), 10)

            query = {"$expr": {"$in": ["$_id", random_indices]}}

            random_documents = list(babynames_names.aggregate([
                {"$match": query},
                {"$sample": {"size": 10}}
            ]))
            recommended_names = document.get('recommendedNames')

            for rd in random_documents:
                name = rd.get('name')
                if name:
                    recommended_names.append(name)
            
            name_key = document.get('name')
            babynames_names.update_one({'name' : name_key}, {'$set' : {'recommendedNames' : recommended_names[0 : 10]}})





BasicColdStartPopularity.random_popularity()