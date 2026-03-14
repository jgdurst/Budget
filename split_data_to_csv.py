from tkinter.filedialog import askopenfilenames
import pandas as pd
import Budget as budget_lib
budget = budget_lib.Budget()
filenames = askopenfilenames(initialdir=f"{budget.get_working_dir()}\\Transactions")
for i,filename in enumerate(filenames):
    print(f"{i}) === Splitting file ({filename}) by column Category")
    df = pd.read_csv(filename)
    split_dfs = {category: group.reset_index(drop=True)
        for category, group in df.groupby('Category')}
    for category, sub_df in split_dfs.items():
        sub_df.to_csv(f"SplitTrans{category}.csv")
    print(f"{i}) === Action Complete\n")

