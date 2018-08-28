const db = {}

// exam results $lookup
db.student_course.aggregate([
  {
    $project: {
      _id: 0,
      user_id: 1
    }
  },
  {
    $lookup: {
      from: "exam_results",
      let: { source: "$user_id" },
      pipeline: [
        {
          $match: {
            $expr: {
              $eq: ["$user_id", "$$source"]
            }
          }
        },
        {
          $project: {
            _id: 0,
            user_id: 1
          }
        }
      ],
      as: "exams"
    }
  },
  {
    $match: {
      $expr: {
        $gt: [{ $size: "$exams" }, 0]
      }
    }
  },
  {
    $sortByCount: { $size: "$exams" }
  }
])

// devs

// just finding out what the problem types even are
db.student_course.aggregate([
  {
    $match: {
      $expr: { $gte: [{ $size: { $objectToArray: "$tab_map" } }, 1] }
    }
  },
  {
    $project: {
      _id: 0,
      tab_map: {
        $objectToArray: "$tab_map"
      }
    }
  },
  {
    $addFields: {
      tab_map: "$tab_map.v"
    }
  },
  {
    $unwind: "$tab_map"
  },
  {
    $match: {
      $expr: {
        $ne: [{ $type: "$tab_map.attempts" }, "missing"]
      }
    }
  },
  {
    $match: {
      $expr: {
        $gte: [{ $size: "$tab_map.attempts" }, 1]
      }
    }
  },
  {
    $project: {
      submission_types: {
        $map: {
          input: "$tab_map.attempts",
          in: {
            $arrayToObject: {
              $filter: {
                input: {
                  $objectToArray: "$$this"
                },
                cond: {
                  $not: { $in: ["$$this.k", ["_cls", "date", "git_stamp"]] }
                }
              }
            }
          }
        }
      }
    }
  },
  {
    $unwind: "$submission_types"
  },
  {
    $project: {
      type: {
        $objectToArray: "$submission_types"
      }
    }
  },
  {
    $project: {
      type: "$type.k"
    }
  },
  {
    $unwind: "$type"
  },
  {
    $group: {
      _id: "$type"
    }
  }
])

// The initial cleansing pipeline to get rid of needless cruft
// Just reduces it down to the offering, problem, number of attempts, and some basic
// attempt info like what the problem type is and what the student's submission(s)
// were

db.student_course.aggregate(
  [
    {
      $match: {
        $expr: { $gte: [{ $size: { $objectToArray: "$tab_map" } }, 1] }
      }
    },
    {
      $project: {
        _id: 0,
        offering: 1,
        user_id: 1,
        tab_map: {
          $objectToArray: "$tab_map"
        }
      }
    },
    {
      $addFields: {
        tab_map: "$tab_map.v"
      }
    },
    {
      $unwind: {
        path: "$tab_map",
        preserveNullAndEmpty: false
      }
    },
    {
      $match: {
        $expr: {
          $ne: [{ $type: "$tab_map.attempts" }, "missing"]
        }
      }
    },
    {
      $project: {
        offering: 1,
        user_id: 1,
        submission: {
          problem_id: "$tab_map.id",
          num_attempts: { $size: "$tab_map.attempts" },
          attempts: {
            $map: {
              input: "$tab_map.attempts",
              in: {
                $arrayToObject: {
                  $filter: {
                    input: {
                      $objectToArray: "$$this"
                    },
                    cond: {
                      $in: [
                        "$$this.k",
                        ["_cls", "text", "indices", "choices", "index"]
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
      $group: {
        _id: {
          offering: "$offering",
          user_id: "$user_id"
        },
        submissions: {
          $push: "$submission"
        }
      }
    },
    {
      $project: {
        _id: 0,
        offering: "$_id.offering",
        user_id: "$_id.user_id",
        submissions: 1
      }
    },
    {
      $out: "courses_etl"
    }
  ],
  { allowDiskUse: true }
)

// now we need to etl the course_info collection to build a correlation so we
// can tell whether a user got the answer correct
// WIP

db.student_course.aggregate(
  [
    {
      $match: {
        offering: "M103/2018_April",
        $expr: { $gte: [{ $size: { $objectToArray: "$tab_map" } }, 1] }
      }
    },
    {
      $project: {
        _id: 0,
        offering: 1,
        user_id: 1,
        tab_map: {
          $objectToArray: "$tab_map"
        }
      }
    },
    {
      $addFields: {
        tab_map: "$tab_map.v"
      }
    },
    {
      $unwind: "$tab_map"
    },
    {
      $match: {
        $expr: {
          $ne: [{ $type: "$tab_map.attempts" }, "missing"]
        }
      }
    },
    {
      $project: {
        offering: 1,
        user_id: 1,
        submission: {
          problem_id: "$tab_map.id",
          num_attempts: { $size: "$tab_map.attempts" },
          attempts: {
            $concatArrays: [
              {
                $map: {
                  input: "$tab_map.attempts",
                  in: {
                    $setUnion: [
                      {
                        $map: {
                          input: {
                            $filter: {
                              input: {
                                $objectToArray: "$$this"
                              },
                              cond: {
                                $in: [
                                  "$$this.k",
                                  ["text", "indices", "choices", "index"]
                                ]
                              }
                            }
                          },
                          in: "$$this.v"
                        }
                      },
                      []
                    ]
                  }
                }
              }
            ]
          }
        }
      }
    },
    {
      $group: {
        _id: {
          offering: "$offering",
          user_id: "$user_id"
        },
        submissions: {
          $push: "$submission"
        }
      }
    },
    {
      $project: {
        _id: 0,
        offering: "$_id.offering",
        user_id: "$_id.user_id",
        submissions: 1
      }
    }
  ],
  { allowDiskUse: true }
)

db.student_course.aggregate(
  [
    {
      $match: {
        $expr: { $gte: [{ $size: { $objectToArray: "$tab_map" } }, 1] }
      }
    },
    {
      $match: {
        $expr: {
          $in: [
            { $substr: ["$offering", 0, 4] },
            ["M036", "M121", "M103", "M201", "M310", "M312", "M001"]
          ]
        }
      }
    },
    {
      $project: {
        _id: 0,
        offering: 1,
        user_id: 1,
        tab_map: {
          $objectToArray: "$tab_map"
        }
      }
    },
    {
      $addFields: {
        tab_map: "$tab_map.v"
      }
    },
    {
      $unwind: {
        path: "$tab_map",
        preserveNullAndEmptyArrays: false
      }
    },
    {
      $match: {
        "tab_map.attempts": { $type: "array" }
      }
    },
    {
      $project: {
        user_id: 1,
        offering: 1,
        submission: {
          problem_id: "$tab_map.id",
          num_attempts: { $size: "$tab_map.attempts" },
          attempts: {
            $map: {
              input: "$tab_map.attempts",
              in: {
                $arrayToObject: {
                  $filter: {
                    input: {
                      $objectToArray: "$$this"
                    },
                    cond: {
                      $in: [
                        "$$this.k",
                        ["_cls", "text", "indices", "choices", "index"]
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
      $group: {
        _id: {
          offering: "$offering",
          user_id: "$user_id"
        },
        submissions: {
          $push: "$submission"
        }
      }
    },
    {
      $project: {
        _id: 0,
        offering: "$_id.offering",
        user_id: "$_id.user_id",
        submissions: 1
      }
    },
    {
      $out: "courses_etl"
    }
  ],
  {
    allowDiskUse: true
  }
)

// test
db.courses_etl.aggregate([
  {
    $unwind: "$submissions"
  },
  {
    $lookup: {
      from: "answers_etl",
      let: { source: "$submissions.problem_id" },
      pipeline: [
        {
          $match: {
            $expr: { $eq: ["$problem_id", "$$source"] }
          }
        },
        {
          $project: {
            _id: 0,
            answer: "$answer"
          }
        }
      ],
      as: "answer"
    }
  },
  {
    $match: {
      answer: { $elemMatch: { $exists: true } }
    }
  },
  {
    $addFields: {
      answer: {
        $cond: [
          { $isArray: ["$answer.answer"] },
          { $arrayElemAt: ["$answer.answer", 0] },
          "$answer.answer"
        ]
      }
    }
  },
  {
    $unwind: "$submissions.attempts"
  },
  {
    $match: {
      "submissions.attempts._cls": {
        $in: [
          "mercury.models.HgCheckAllThatApplyAttempt",
          "HgCheckAllThatApplyAttempt"
        ]
      },
      $expr: { $not: [{ $isArray: "$answer" }] }
    }
  }
])

db.courses_etl.aggregate(
  [
    {
      $unwind: "$submissions"
    },
    {
      $lookup: {
        from: "answers_etl",
        let: { source: "$submissions.problem_id" },
        pipeline: [
          {
            $match: {
              $expr: { $eq: ["$problem_id", "$$source"] }
            }
          },
          {
            $project: {
              _id: 0,
              answer: "$answer"
            }
          }
        ],
        as: "answer"
      }
    },
    {
      $match: {
        answer: { $elemMatch: { $exists: true } }
      }
    },
    {
      $addFields: {
        answer: {
          $cond: [
            { $isArray: ["$answer.answer"] },
            { $arrayElemAt: ["$answer.answer", 0] },
            "$answer.answer"
          ]
        }
      }
    },
    {
      $addFields: {
        submissions: {
          problem_id: "$submissions.problem_id",
          num_attempts: "$submissions.num_attempts",
          attempts: {
            $map: {
              input: "$submissions.attempts",
              in: {
                $switch: {
                  branches: [
                    {
                      case: {
                        $and: [
                          {
                            $in: [
                              "$$this._cls",
                              [
                                "HgCheckAllThatApplyAttempt",
                                "mercury.models.HgCheckAllThatApplyAttempt"
                              ]
                            ]
                          },
                          {
                            $isArray: "$answer"
                          }
                        ]
                      },
                      then: {
                        _cls: "$$this.cls",
                        indices: "$$this.indices",
                        correct: {
                          $and: [
                            {
                              $reduce: {
                                input: "$$this.indices",
                                initialValue: true,
                                in: {
                                  $cond: [
                                    {
                                      $eq: [
                                        { $arrayElemAt: ["$answer", "$$this"] },
                                        1
                                      ]
                                    },
                                    { $and: ["$$value", true] },
                                    { $and: ["$$value", false] }
                                  ]
                                }
                              }
                            },
                            {
                              $eq: [
                                { $size: "$$this.indices" },
                                {
                                  $size: {
                                    $filter: {
                                      input: "$answer",
                                      cond: {
                                        $eq: ["$$this", 1]
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
                      case: {
                        $in: [
                          "$$this._cls",
                          [
                            "HgMultipleChoiceAttempt",
                            "mercury.models.HgMultipleChoiceAttempt"
                          ]
                        ]
                      },
                      then: {
                        _cls: "$$this._cls",
                        index: "$$this.index",
                        correct: { $eq: ["$answer", "$$this.index"] }
                      }
                    }
                  ],
                  default: {
                    _cls: "$$this._cls",
                    text: "$$this.text",
                    correct: { $eq: ["$answer", "$$this.text"] }
                  }
                }
              }
            }
          }
        }
      }
    },
    {
      $project: {
        _id: 0,
        offering: 1,
        user_id: 1,
        submissions: 1
      }
    },
    {
      $group: {
        _id: {
          offering: "$offering",
          user_id: "$user_id"
        },
        submissions: {
          $push: "$submissions"
        }
      }
    },
    {
      $out: "correlated_answers"
    }
  ],
  { allowDiskUse: true }
)
