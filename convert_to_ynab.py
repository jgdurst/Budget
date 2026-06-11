from tkinter.filedialog import askopenfilenames
import pandas as pd
import os
from pathlib import Path

optional_cols = ['Posting Date','Post Date','Description','Amount']
output_cols = ['Date','Payee','Amount']
rename_dict = {
    "Description": "Payee"
}

file_paths = askopenfilenames(initialdir=r"C:\Users\jgdur\Downloads")
for i,file_path_str in enumerate(file_paths):
    file_path = Path(file_path_str)
    print(f"{i}) === Converting ({file_path}) to YNAB format.")
    header = pd.read_csv(file_path, nrows=0).columns.tolist()
    available_cols = [col for col in header if col in optional_cols]
    for col in available_cols:
        if (col == 'Posting Date'):
            rename_dict['Posting Date'] = 'Date'
        if (col == 'Post Date'):
            rename_dict['Post Date'] = 'Date'

    df = pd.read_csv(file_path, index_col=False)
    # print(df.iloc[0])
    df.rename(columns=rename_dict, inplace=True)
    print(df[output_cols].iloc[0])

    dir_path = os.path.dirname(file_path)
    file_basename = os.path.splitext(os.path.basename(file_path))[0]
    new_file_path = f"{dir_path}\\ynab_{file_basename}.csv"
    df[output_cols].to_csv(new_file_path, index=False, header=output_cols)
    
print(f"{i}) === Conversions Complete\n")

