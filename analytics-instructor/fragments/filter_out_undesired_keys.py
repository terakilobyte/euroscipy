transform_attempt = {
    "$project": {
        "_id": 0,
        "attempt": {"$objectToArray": "$$ROOT"}
    }
}
keys_to_keep = ["_cls", "text", "indices", "choices", "index"]
filter_keys = {
    "$filter": {
        "input": "$attempt",
        "cond": {
            "$in": ["$$this.k", keys_to_keep]
        }
    }
}
filter_attempt = {
    "$addFields": {
        "attempt": filter_keys
    }
}
m_print(my_coll.aggregate([transform_attempt, filter_attempt]), justone=True)
