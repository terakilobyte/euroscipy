predicate = {
    "$match": {
        "offering": { "$in": ["M103/2018_April", "M121/2018_April"] },
        "user_id": { "$gte": 75000, "$lt": 150000 }
    }
}
