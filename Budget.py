import pandas as pd
import numpy as np
import os
import pyodbc
import math
import datetime as dt
from datetime import datetime
import yaml


budget_path = r'C:\Users\James\OneDrive\Budget'
budget_file_path = budget_path + '\\Budget Files'
db_file_name = 'Budget_v2.accdb' #deprecated
db_file_path = budget_path + f'\\{db_file_name}' #deprecated
connection_string = r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;' % (db_file_path)
config_file = "config.yml"

def get_config():
    if os.path.exists(config_file):
        with open(config_file, "r") as config_fstream:
            try:
                config = yaml.safe_load(config_fstream)
                print(config['database']['connection_string'])
            except Exception as e:
                print(f"Failed to read {config_file}")
                print(e)
    else:
        print(f"'{config_file}' does not exist. Rerun ConnectDatabase")

def import_transactions(file_name, logging=False):
    print(f"Importing transactions...")
    chase_date_parser = lambda x: datetime.strptime(x, '%m/%d/%Y')
    file_path = file_name

    transactions = pd.read_csv(file_path, parse_dates=['TransactionDate'], date_parser=chase_date_parser,
                           usecols=['TransactionDate', 'Description', 'Amount', 'Check', 'Account', 'Comment', 'Category'],
                           keep_default_na=False
                           )
    transactions['Check'] = 0
    transactions['Description'] = transactions['Description'].map(lambda x: " ".join(str(x).split()).replace("'",""))

    try:
        transactions['TransID'] = transactions['TransactionDate'].dt.date.map(str) \
                             + '_' \
                             + transactions['Amount'].map('{:0=10.2f}'.format).str.replace('-', 'n') \
                             + '_' \
                             + transactions['Description'].map(lambda x: x.replace(' ', '').replace('_','+'))
    except:
        print(f'Invalid data values in Date, Description, or Amount columns. {len(transactions.index)} rows found.')
        return None

    i = 1
    duplicated = True
    while duplicated:
        trans_duplicates = transactions["TransID"].duplicated()
        if trans_duplicates.any():
            if i == 1:
                transactions["TransID"][trans_duplicates] = \
                    transactions["TransID"][trans_duplicates] + f"-{i}"
            else:
                transactions["TransID"][trans_duplicates] = \
                    transactions["TransID"][trans_duplicates].map(lambda x: x[:-2] + f"-{i}")
            i += 1
        else:
            duplicated = False

    transactions['IsParentTrans'] = transactions.apply(lambda row: '|' in row.Category, axis=1)
    return transactions

def import_allocations(file_name):
    file_path = f"{budget_file_path}\\Allocations\\{file_name}"
    allocations = pd.read_csv(file_path)
    expected_cols = {'Category','AllocationYear','AllocationMonth', 'Amount'}
    if (expected_cols.issubset(allocations.columns)):
        return allocations
    else:
        print(f'Expected columns not found in dataset:\n{expected_cols}')
        return None

def insert_transactions(trans_to_insert, logging=False):
    print(f"Uploading transactions... ")

    if trans_to_insert is None:
        print("No data to insert.")
        return None

    #get accounts in import data
    accounts = trans_to_insert['Account'].unique()

    #connect to database
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    for account in accounts:
        cursor.execute(
            f'''
            SELECT Account.Account FROM Account
            WHERE Account.Account = '{account}';
            '''
        )
        if not cursor.fetchone():
            print(f'{trans_to_insert.TransID}\n--\'{account}\' is not a valid Account.')
            return

    cnt_new = 0
    cnt_exist = 0
    cnt_invalid_subtrans = 0
    cnt_invalid_category = 0
    cnt_invalid_tag = 0

    for trans in trans_to_insert.itertuples():
        try:
            check = 'NULL' if math.isnan(trans.Check) else str(trans.Check)
        except:
            check = 'NULL' if not trans.Check else trans.Check
        invalid_subtrans = False
        invalid_tag = False
        sum_subtrans_amount = 0
        category_spend_dict = {}
            
        if trans.IsParentTrans:
            adj_category = ''
            trans_category_list = trans.Category.split('|')
            for val in trans_category_list:
                if (val.strip()):
                    print("'" + val + "'")
                    category = val[:val.find('(')].strip()
                    amount = val[val.find('(') + 1:val.find(')')].strip()
                    cursor.execute(
                        f'''
                        SELECT Category.Category FROM Category
                        WHERE Category.Category = '{category}';
                        '''
                    )
                    if not cursor.fetchone():
                        invalid_subtrans = True
                        cnt_invalid_subtrans += 1
                        break
                    else:
                        sum_subtrans_amount += float(amount)
                        category_spend_dict[category] = amount

        else:
            cursor.execute(
                f'''
                SELECT Category.Category FROM Category
                WHERE Category.Category = '{trans.Category}';
                '''
            )
            if trans.Category and not (cursor.fetchone()):
                if logging:
                    print('Invalid Category')
                cnt_invalid_category += 1
                print(f'{trans.TransID}\n--\'{trans.Category}\' is not a valid Category.')
                continue
            adj_category = trans.Category

        if (not (invalid_subtrans)):
            try:
                if logging:
                    print(f"{trans.TransID}, {trans.TransactionDate}, {trans.Description}, {trans.Amount}, {trans.Comment}, {check}, {trans.Account}, {adj_category}")
                cursor.execute(
                    f'''
                    INSERT INTO Transaction (TransID, TransactionDate, Description, Amount, Comment, Check, Account, Category)
                    VALUES ('{trans.TransID}', '{trans.TransactionDate}', '{trans.Description}', {trans.Amount}, '{trans.Comment}', {check}, '{trans.Account}', '{adj_category}');
                    '''
                )
                cnt_new += 1

                if trans.IsParentTrans:
                    for category, amount in category_spend_dict.items():
                        cursor.execute(
                            f'''
                            INSERT INTO SubTransaction (ParentTransID, Category, Amount)
                            VALUES ('{trans.TransID}', '{category}', {amount});
                            '''
                        )
                
            except pyodbc.IntegrityError:
                if logging:
                    print('Duplicate')
                cnt_exist += 1
                # print(f'{trans.TransID}\n--Record already exists.')
                continue

    conn.commit()
    print(f"Transactions upload completed.")
    print(f'Total Records: {len(trans_to_insert.index)}')
    if cnt_new > 0:
        print(f'New records: {cnt_new}')
    if cnt_exist > 0:
        print(f'Duplicates: {cnt_exist}')
    if cnt_invalid_category > 0:
        print(f'Records with Invalid Categories: {cnt_invalid_category}')
    if cnt_invalid_subtrans > 0:
        print(f'Records with Invalid Subtransactions: {cnt_invalid_subtrans}')
    if cnt_invalid_tag > 0:
        print(f'Records with Invalid Tags: {cnt_invalid_tag}')

def single_allocation(category, year, month, amount, alloc_type):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute(
        f'''
        SELECT NULL
        FROM Allocation
        WHERE (
            Allocation.Category = '{category}'
        );
        '''
    )
    if not cursor.fetchone():
        raise Exception("Category not valid.")
    
    cursor.execute(
        f'''
        SELECT NULL
        FROM Allocation
        WHERE (
            Allocation.AllocationYear = {year}
        )
        '''
    )
    if not cursor.fetchone():
        _insert_new_year(year)

    if (alloc_type == 'add'):
        alloc_amount_str = f'Allocation.Amount + {amount}'
    elif (alloc_type == 'set'):
        alloc_amount_str = f'{amount}'

    cursor.execute(
        f'''
        UPDATE Allocation 
        SET Allocation.Amount = {alloc_amount_str}
        WHERE (
            Allocation.Category = '{category}' AND
            Allocation.AllocationYear = {year} AND
            Allocation.AllocationMonth = {month}
        );
        '''
    )
    conn.commit()
#TODO
def mass_allocation(alloc_data, alloc_type):
    i = 0
    size = alloc_data.shape[0]
    print(size, "rows to update")
    print("_" * 50)
    unit = size / 50
    prev_unit = 0
    for row in alloc_data.iterrows():
        if i / unit > prev_unit:
            print("#", end="")
            prev_unit += 1
        single_allocation(
            row[1]["Category"],
            row[1]["AllocationYear"],
            row[1]["AllocationMonth"],
            row[1]["Amount"],
            alloc_type
        )
        i+=1
    print(f"{size} rows updated.")


def get_allocations_by_period(year, month):
    conn = pyodbc.connect(connection_string)

    query = (
        f'''
        SELECT Category.ID, Category.Section, Category.Category, Allocation.Amount
        FROM Allocation 
        LEFT JOIN Category 
        ON Allocation.Category = Category.Category
        WHERE 
        (
            (Allocation.AllocationYear)={year} AND 
            (Allocation.AllocationMonth)={month}
        );
        '''
    )

    return pd.read_sql(query, conn)

def get_fdata(tablename, filters):
    conn = pyodbc.connect(connection_string)

    criteria = [f"{key} IN ({value})" for key, value in filters.items()]
    str_criteria = "AND".join(criteria)

    query = (
        f'''
        SELECT * FROM {tablename}
        WHERE {str_criteria};
        '''
    )
    print(query)

    return pd.read_sql(query, conn)

def get_data(tablename):
    conn = pyodbc.connect(connection_string)

    query = (
        f'''
        SELECT * FROM {tablename};
        '''
    )

    return pd.read_sql(query, conn)

def validate_unique_trans(df_validate):
    df_trans = get_data('Transaction')
    for row in df_validate.itertuples():
        if (df_trans['TransID'].isin([row.TransID]).any()):
            print('Record already exists in DB: ' + row.TransID)

def _insert_new_year(year):
    conn = pyodbc.connect(connection_string)

    cursor = conn.cursor()

    for i in range(1, 13):
        cursor.execute(
            f'''
            SELECT NULL
            FROM Timeline
            WHERE Timeline.BudgetYear = {year} AND Timeline.BudgetMonth = {i};
            '''
        )
        if not cursor.fetchone():
            cursor.execute(
                f'''
                INSERT INTO Timeline (BudgetYear, BudgetMonth, BudgetMonthDesc)
                VALUES ({year}, {i}, '{dt.date(year, i, 1).strftime('%b')}');
                '''
            )
        conn.commit()
        _insert_allocation_categories(year, i)

def _insert_allocation_categories(year, month):
    category_data = get_data("Allocation")

    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    for row in category_data.itertuples():
        cursor.execute(
            f'''
            SELECT NULL 
            FROM Allocation
            WHERE (
                Allocation.AllocationYear = {year} AND 
                Allocation.AllocationMonth = {month} AND 
                Allocation.Category = '{row.Category}'
            );
            '''
        )
        if not cursor.fetchone():
            cursor.execute(
                f'''
                INSERT INTO Allocation (Category, Section, AllocationYear, AllocationMonth, Amount)
                VALUES ('{row.Category}', '{row.Section}', {year}, {month}, 0);
                '''
            )

    conn.commit()

def get_current_year():
    return dt.date.today().year

def get_current_month():
    return dt.date.today().month

def get_month_name(month):
    return dt.date(get_current_year(), month, 1).strftime('%b')

#TODO convert to single get that perform this, maybe one summary and one row-wise?
def get_trans_by_period(year, month):
    conn = pyodbc.connect(connection_string)

    query = (
        f'''
        SELECT * FROM Transaction
        WHERE Transaction.TransactionYear = {year} AND Transaction.TransactionMonth = {month};
        '''
    )

    return pd.read_sql(query, conn)

def get_trans_summ_by_period(year, month):
    conn = pyodbc.connect(connection_string)

    query = (
        f'''
        SELECT Category.Category, Transaction.TransactionMonth, Transaction.TransactionYear, Sum(Transaction.Amount) AS SumAmount
        FROM Category LEFT JOIN [Transaction] ON Category.ID = Transaction.Category
        WHERE Transaction.TransactionMonth = {month} AND Transaction.TransactionYear = {year}
        GROUP BY Category.Category, Transaction.TransactionMonth, Transaction.TransactionYear;
        '''
    )

    return pd.read_sql(query, conn)

if __name__ == '__main__':
    # alloc = import_allocations("Allocations.csv")
    # mass_allocation(alloc, "set")
    # test_transactions = import_transactions("Checking", "Checking Transactions test.csv")
    # print(test_transactions)
    # print(test_transactions['Comment'])
    # print(test_transactions.iloc[0])
    pass
