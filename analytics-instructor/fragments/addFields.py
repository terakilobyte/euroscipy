add_sales_information = {
    "$addFields": {
        "avg_quarterly_sales": {"$avg": "$yearly_sales.sales"},
        "total_yearly_sales": {"$sum": "$yearly_sales.sales"}
    }
}
m_print(my_coll.aggregate([add_sales_information])
