import Budget as budget_lib
budget = budget_lib.Budget()

for i in range(2014,2025):
    for j in range(1,13):
        budget._insert_allocation_categories(i,j)

