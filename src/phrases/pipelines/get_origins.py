
def pipeline(o, start=None,end=None):
   if start and end:
      return [
        {
            '$match':{
               'timestamp': {
                  '$gte' : start,
                  '$lte' : end
               }
            }
        },
        {
        "$lookup": {
            "from": "names",
            "localField": "name",
            "foreignField": "name",
            "as": "name_details"
        }
    },
    {
        "$unwind": "$name_details"
    },
    {
        "$match": {
            "name_details.origin": o
        }
    },
    {
        "$project": {
            "_id": 0,
            "relationalName": "$name_details.name"
        }
    }
      ]
   return [
    {
        "$lookup": {
            "from": "names",
            "localField": "name",
            "foreignField": "name",
            "as": "name_details"
        }
    },
    {
        "$unwind": "$name_details"
    },
    {
        "$match": {
            "name_details.origin": o
        }
    },
    {
        "$project": {
            "_id": 0,
            "relationalName": "$name_details.name"
        }
    }
]
