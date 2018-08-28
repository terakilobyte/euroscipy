student_course = {}
course_info = {}
courses_etl = {}
db = {}

# courses_etl
db.student_course.aggregate(
    [
        {
            "$match": {
                "$expr": {"$gte": [{"$size": {"$objectToArray": "$tab_map"}}, 1]},
                "$expr": {"$in": [{"$substr": ["$offering", 0, 4]}, ["M036", "M121", "M103", "M201", "M310", "M312", "M001"]]}
            }
        },
        {
            "$project": {
                "_id": 0,
                "offering": 1,
                "user_id": 1,
                "tab_map": {
                    "$objectToArray": "$tab_map"
                }
            }
        },
        {
            "$addFields": {
                "tab_map": "$tab_map.v"
            }
        },
        {
            "$unwind": {
                "path": "$tab_map",
                "preserveNullAndEmptyArrays": false
            }
        },
        {
            "$match": {
                "tab_map.attempts": {"$type": "array"}
            }
        },
        {
            "$project": {
                "user_id": 1,
                "offering": 1,
                "submission": {
                    "problem_id": "$tab_map.id",
                    "num_attempts": {"$size": "$tab_map.attempts"},
                    "attempts": {
                        "$map": {
                            "input": "$tab_map.attempts",
                            "in": {
                                "$arrayToObject": {
                                    "$filter": {
                                        "input": {
                                            "$objectToArray": "$$this"
                                        },
                                        "cond": {
                                            "$in": [
                                                "$$this.k",
                                                ["_cls", "text", "indices",
                                                 "choices", "index"]
                                            ]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "offering": "$offering",
                    "user_id": "$user_id"
                },
                "submissions": {
                    "$push": "$submission"
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "offering": "$_id.offering",
                "user_id": "$_id.user_id",
                "submissions": 1
            }
        },
        {
            "$out": "courses_etl_test"
        }
    ],
    {
        "allowDiskUse": true
    }
)

# answers_etl
db.course_info.aggregate([
    {
        "$project": {
            "_id": 0,
            "problems": "$course.chapters.lessons.problem"
        }
    },
    {
        "$unwind": {
            "path": "$problems",
            "preserveNullAndEmptyArrays": false
        }
    },
    {
        "$unwind": {
            "path": "$problems",
            "preserveNullAndEmptyArrays": false
        }
    },
    {
        "$match": {
            "problems": {"$ne": null}
        }
    },
    {
        "$project": {
            "problem_id": "$problems.url_name",
            "answer": {
                "$switch": {
                    "branches": [
                        {
                            "case": {
                                "$eq": ["$problems.class", "MultipleChoiceProblem"]
                            },
                            "then": {
                                "$indexOfArray": ["$problems.choices.is_correct", true]
                            }
                        },
                        {
                            "case": {
                                "$eq": ["$problems.class", "CheckAllThatApplyProblem"]
                            },
                            "then": {
                                "$map": {
                                    "input": "$problems.choices",
                                    "in": {
                                        "$cond": [
                                            {"$eq": [
                                                "$$this.is_correct", true]},
                                            1,
                                            0
                                        ]
                                    }
                                }
                            }
                        }
                    ],
                    "default": "$problems.answer"
                }
            }
        }
    },
    {
        "$group": {
            "_id": "$problem_id",
            "answer": {"$last": "$answer"}
        }
    },
    {
        "$project": {
            "answer": "$answer",
            "_id": 0,
            "problem_id": "$_id"
        }
    },
    {
        "$out": "answers_etl"
    }
])

courses_etl.aggregate([
    {
        "$unwind": "$submissions"
    },
    {
        "$lookup": {
            "from": "answers_etl",
            "let": {"source": "$submissions.problem_id"},
            "pipeline": [
                {
                    "$match": {
                        "$expr": {"$eq": ["$problem_id", "$$source"]}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "answer": 1
                    }
                }
            ],
            "as": "answer"
        }
    },
    {
        "$match": {
            "answer": {"$elemMatch": {"$exists": True}}
        }
    },
    {
        "$addFields": {
            "answer": {
                "$arrayElemAt": ["$answer.answer", 0]
            }
        }
    },
    {
        "$addFields": {
            "status": {
                "$map": {
                    "input": "$submissions.attempts",
                    "in": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {
                                        "$in": [
                                            "$$this._cls",
                                            [
                                                "HgCheckAllThatApplyAttempt",
                                                "mercury.models.HgCheckAllThatApplyAttempt"
                                            ]
                                        ]
                                    },
                                    "then": {
                                        "status": {
                                            "$and": [
                                                {
                                                    "$reduce": {
                                                        "input": "$$this.indices",
                                                        "initialValue": True,
                                                        "in": {
                                                            "$cond": [
                                                              {
                                                                  "$eq": [
                                                                      {"$arrayElemAt": [
                                                                          "$answer", "$$this"]},
                                                                      1
                                                                  ]
                                                              },
                                                                {"$and": [
                                                                    "$$value", True]},
                                                                {"$and": [
                                                                    "$$value", False]}
                                                            ]
                                                        }
                                                    }
                                                },
                                                {
                                                    "$eq": [
                                                        {"$size": "$$this.indices"},
                                                        {
                                                            "$size": {
                                                                "$filter": {
                                                                    "input": "$answer",
                                                                    "cond": {
                                                                        "$eq": ["$$this", 1]
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    }
                                },
                                {
                                    "case": {
                                        "$in": [
                                            "$$this._cls",
                                            [
                                                "HgMultipleChoiceAttempt",
                                                "mercury.models.HgMultipleChoiceAttempt"
                                            ]
                                        ]
                                    },
                                    "then": {
                                        "status": {"$eq": ["$answer", "$$this.index"]}
                                    }
                                }
                            ],
                            "default": {"status": {"$eq": ["$answer", "$$this.text"]}}
                        }
                    }
                }
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "user_id": 1,
            "offering": 1,
            "submissions": {
                "$zip": {
                    "inputs": ["$submissions.attempts", "$status"]
                }
            }
        }
    },
    {
        "$out": "correlated_answers"
    }
])
