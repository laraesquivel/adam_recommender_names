
def pipeline(r,o,start=None, end = None):
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
            "location_mat.region": r ,
            "name_details.origin": o

        }
    },
        {
        "$project": {
            "_id": 0,
            "relationalName": "$names_datails.name"
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
            "location_mat.region": r ,
            "name_details.origin": o

        }
    },
        {
        "$project": {
            "_id": 0,
            "relationalName": "$names_datails.name"
        }
    }
    ]