"""
INSTRUCTORS AND TAS:

Feel free to jump start students, encouraging them to look at the source data.
Maybe perform some explatory queries. One example might be to group just on the
offerings. How many are there? Maybe we should group on a **substring** of the
offering name, like the course code?

Why the double group? The first group will give us back documents representing
the cumulative for a problem in a course along with the average attempts for
that problem.

The second group collects all of that information into course groupings,
pushing the problem_id and average attempts into an array.

If students are confused about this point have them only perform the first
group and look at the output (without the visualization).
"""


"""
Begin by unwinding the submissions arrays within each document.
This will give us one document per submission.
"""
unwind_submissions = {
    "$unwind": "$submissions"
}

"""
Then group by the short offering name (M001, M312, etc...) as well as the
problem_id
calculate the average attempts for this problem
"""
group_by_offering_and_problem = {
    "$group": {
        "_id": {
            "offering": {
                "substrBytes": ["$_id.offering", 0, 4]
            },
            "problem_id": "$submissions.problem_id"
        },
        "avg_attempts": {
            "$avg": "$submissions.num_attempts"
        }
    }
}

"""
Now that we have problems grouped together, let's regroup them so that
they are all grouped together just by their offering
"""

group_by_offering = {
    "$group": {
        "_id": "$_id.offering",
        "problems": {
            "$push": {
                "problem_id": "$_id.problem_id",
                "avg_attempts": "$avg_attempts"
            }
        }
    }
}
