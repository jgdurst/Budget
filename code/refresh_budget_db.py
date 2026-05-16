from tkinter.filedialog import askopenfilename
import Budget as budget_lib
budget = budget_lib.Budget()

timeline_df = budget.get_data('Timeline')
budget_years = timeline_df['BudgetYear'].unique().tolist()
budget_years.sort()
for year in budget_years:
    budget._insert_new_year(year)
