keys_to_keep = ["_cls", "text", "indices", "choices", "index"]
filter_keys = {
    "$filter": {
        "input": {"$objectToArray": "$$CURRENT"},
        "cond": {
            "$in": ["$$this.k", keys_to_keep]
        }
    }
}
filter_attempt = {
    "$project": {
        "_id": 0,
        "attempt": filter_keys
    }
}
m_print(my_coll.aggregate([filter_attempt]), justone=True)
