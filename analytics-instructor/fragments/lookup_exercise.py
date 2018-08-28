only_m103_april = {
    "$match": {
        "offering": "M103/2018_April"
    }
}
just_user_id = {
    "$project": {
        "_id": 0,
        "user_id": 1
    }
}

lookup = {
    "$lookup": {
        "from": "user_profile",
        "let": {"source": "$user_id"},
        "pipeline": [
            {
                "$match": {
                    "$expr": {
                        "$eq": ["$user_id", "$$source"]
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "devlang": 1
                }
            }
        ],
        "as": "devlangs"
    }
}
unwind_devlangs = {
    "$unwind": "$devlangs"
}
sort_devlangs = {
    "$sortByCount": "$devlangs"
}

m_print(student_course.aggregate(
    [only_m103_april, just_user_id, lookup, unwind_devlangs, sort_devlangs]
))
