compute_tab_map_size = {
    "$addFields": {
        "tab_map_size": {
            "$gte": [{"$size": {"$objectToArray": "$tab_map"}}, 1]
        }
    }
}
filter_out_no_tab_map = {
    "$match": {
        "tab_map_size": True
    }
}
m_len(collection=student_course, aggregation=[
      compute_tab_map_size, filter_out_no_tab_map])
