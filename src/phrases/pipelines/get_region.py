def pipeline(r,start=None,end=None):
       if start and end:
              return [
                     {
                            '$match'  : {
                                   'timestamp' : {
                                          '$gte' : start,
                                          '$lte' : end
                                   }
                            }
                     },
                         {
        "$lookup": {
            "from": "locations",
            "localField": "location",
            "foreignField": "_id",
            "as": "location_mat"
        }
    },
    {
        "$unwind": "$location_mat"
    },
    {
        "$match": {
            "location_mat.region": r 
        }
    },
    {
        "$project": {
            "_id": 0,
            "name": '$location_mat.name'
        }
    }
              ]
       return [
    {
        "$lookup": {
            "from": "locations",
            "localField": "location",
            "foreignField": "_id",
            "as": "location_mat"
        }
    },
    {
        "$unwind": "$location_mat"
    },
    {
        "$match": {
            "location_mat.region": r 
        }
    },
    {
        "$project": {
            "_id": 0,
            "relationalName": '$location_mat.name'
        }
    }
]
