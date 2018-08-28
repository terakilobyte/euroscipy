attempts_size_no_error = pipeline[:]
ensure_attempts = {
    "$match": {
        "tab_map.attempts": {"$type": "array"}
    }
}
attempts_size_no_error.extend([ensure_attempts, calculate_num_attempts])
m_print(student_course.aggregate(attempts_size_no_error), justone=True)
