"""
This is very similar to the last capstone until the final $lookup stage.

If they get stuck on the $lookup part, encourage them to perform a lookup in
isolation to form the inner pipeline to use.

Remind them to build iteratively!
"""

"""
Again, students should unwind $submissions
"""
unwind_submissions = {
    "$unwind": "$submissions"
}

"""
We again group by problem and offering. Unlike the last capstone, rather than
figure out the average attempts, we $push the user_id into a field.

Why not $addToSet? If we used $addToSet we'd remove information about students
who took a course more than once.
"""
group_by_problem_and_offering = {
    "$group": {
        "_id": {
            "offering": {
                "$substrBytes": ["$_id.offering", 0, 4]
            },
            "problem": "$submissions.problem_id"
        },
        "users_attempted": {
            "$push": "$_id.user_id"
        }
    },
}

"""
Remapping from an array of user_id's to a number that is the size of that array
"""
get_users_attempted_size = {
    "$addFields": {
        "users_attempted": {"$size": "$users_attempted"}
    }
}

"""
Again grouping by offering and pushing the other information into the problems
field.
"""
group_by_offering = {
    "$group": {
        "_id": "$_id.offering",
        "problems": {
            "$push": {
                "problem_id": "$_id.problem",
                "users_attempted": "$users_attempted"
            }
        }
    }
}

"""
Up until now the problem is remarkably similar to the last capstone. Here, we
are using a lookup to get the problem ordering by chapter.

In the $project stage of the pipeline, we map over the $course.chapters array,
returning the value of the "url_name"s. These are the problem_ids.
"""
get_problem_order = {
    "$lookup": {
        "from": "course_info",
        "let": {"offering": "$_id"},
        "pipeline": [
            {
                "$match": {
                    "$expr": {
                        "$eq": [
                            "$$offering",
                            "$course.number"
                        ]
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "chapters": {
                        "$map": {
                            "input": "$course.chapters",
                            "in": "$$this.lessons.problem.url_name"
                        }
                    }
                }
            },
        ],
        "as": "problem_order"
    }
}
