exams_by_student = [
    {
        "$project": {
            "_id": 0,
            "user_id": 1
        }
    },
    {
        "$lookup": {
            "from": "exam_results",
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
                        "user_id": 1
                    }
                }
            ],
            "as": "exams"
        }
    },
    {
        "$match": {
            "$expr": {
                "$gt": [{"$size": "$exams"}, 0]
            }
        }
    },
    {
        "$sortByCount": {"$size": "$exams"}
    }
]
