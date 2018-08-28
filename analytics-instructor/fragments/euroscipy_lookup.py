exams_by_student = [
    {
        "$project": {
            "_id": 0,
            "user_id": 1
        }
    },
    {
        "$lookup": {
            "from": "exam_results_etl",
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
                    "$count": "exams"
                }
            ],
            "as": "exams"
        }
    },
    {
        "$match": {
            "exams": { "$ne": [] }
        }
    },
    {
        "$addFields": {
            "exams": { "$arrayElemAt": ["$exams.exams", 0]}
        }
    },
    {
        "$sort": {"exams": -1}
    }
]
m_print(student_course.aggregate(exams_by_student))
