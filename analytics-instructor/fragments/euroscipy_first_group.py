submissions = {
    "$group": {
        "_id": {
            "$dayOfWeek": "$updated_at"
        },
        "count": { "$sum": 1 }
    }
}
m_print(student_course.aggregate([submissions]))
