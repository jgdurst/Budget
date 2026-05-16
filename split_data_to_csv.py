from tkinter.filedialog import askopenfilenames
import pandas as pd
filenames = askopenfilenames(initialdir=r"C:\Users\jgdur\OneDrive\Budget\Budget Files")
for i,filename in enumerate(filenames):
    print(f"{i}) === Splitting file ({filename}) by column Category")
    df = pd.read_csv(filename)
    split_dfs = {category: group.reset_index(drop=True)
        for category, group in df.groupby('Category')}
    j = 0 
    for category, sub_df in split_dfs.items():
        print(f"{category}")
        sub_df.to_csv(rf"C:\Users\jgdur\OneDrive\Budget\Budget Files\dfmm\SplitTrans{category}{j}.csv")
        j += 1
    print(f"{i}) === Action Complete\n")

