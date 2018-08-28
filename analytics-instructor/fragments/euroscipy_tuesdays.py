mark_day_of_week = {
    "$addFields": {
        "dayofweek": {"$dayOfWeek": "$updated_at"}
    }
}

# remember that Sunday is 1, Saturday is 7
on_tuesday = {
    "$match": {
        "dayofweek": 2
    }
}

# don't change this count stage
count = {
    "$count": "submissions"
}

m_print(student_course.aggregate([mark_day_of_week, on_tuesday, count]))
