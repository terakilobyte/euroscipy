no_empty_tab_maps = {
    "$match": {
        "$expr": {"$gt": [{"$size": {"$objectToArray": "$tab_map"}}, 0]}
    }
}
m_len(collection=student_course, aggregation=[no_empty_tab_maps])
