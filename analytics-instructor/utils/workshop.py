import pandas as pd
from bson import ObjectId, json_util
from pymongo.command_cursor import CommandCursor
from datetime import datetime
from json import JSONEncoder, dumps


def get_dists(collection, day):
    return list(collection)


def get_distribution_for_tuesdays(collection):
    data = collection.aggregate([
        {"$match": {"$expr": {"$eq": [{"$dayOfWeek": "$updated_at"}, 3]}}},
        {"$sortByCount": {"$hour": "$updated_at"}}
    ])

    mapped = [{"hour": entry["_id"], "count":entry["count"]}
              for entry in list(data)]

    return pd.DataFrame.from_dict(mapped)


class MongoJsonEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(obj, ObjectId):
            return str(obj)
        return json_util.default(obj, json_util.CANONICAL_JSON_OPTIONS)


def m_print(to_encode, justone=False):
    if isinstance(to_encode, CommandCursor):
        if justone:
            return print(dumps(to_encode.next(), cls=MongoJsonEncoder, indent=2))
        return print(dumps(list(to_encode), cls=MongoJsonEncoder, indent=2))
    return print(dumps(to_encode, cls=MongoJsonEncoder, indent=2))


def m_print_with_limit(collection=None, aggregation=[], limit=5):
    if collection is None:
        return print('You must specify a colleciton')
    aggregation.append({"$limit": limit})
    return m_print(collection.aggregate(aggregation))


def m_len(collection=None, aggregation=[]):
    if collection is None:
        return print('You must specify a collection')
    aggregation.append({"$count": "count"})
    return print(collection.aggregate(aggregation).next()['count'])
