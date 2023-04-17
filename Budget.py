import pandas as pd
import numpy as np
import os
import pyodbc
import math
import datetime as dt
from datetime import datetime


#Checking/Savings Columns:
# Details (Debit/Credit) --NO
# Posting Date --YES
# Description  --YES
# Amount --YES
# Type (ATM, ACH_Credit/Debit) --NO
# Balance --NO
# Check or Slip # --YES

#Credit Card Columns:
# Transaction Date --NO
# Post Date --YES
# Description --YES
# Category (Chase's guess) --NO
# Type (Payment, Sale, Return) --NO
# Amount --YES

budget_path = r'C:\Users\James\OneDrive\Budget'
budget_file_path = budget_path + "\\Budget Files"
db_file_name = 'Budget_v2.accdb'
db_file_path = budget_path + f'\\{db_file_name}'
connection_string = r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;' % (db_file_path)



def import_transactions(account, file_name):
    print(f"Importing transactions... {account}")
    chase_date_parser = lambda x: datetime.strptime(x, '%m/%d/%Y')
    file_path = f'{budget_file_path}\\{account} Transactions\\{file_name}'

    if (account == 'Checking' or account == 'Savings'):
        #import checking or savings transactions
        transactions = pd.read_csv(file_path, parse_dates=['Posting Date'], date_parser=chase_date_parser,
                                   usecols=['Posting Date', 'Description', 'Amount', 'Check or Slip #', 'Comment', 'Tags', 'Category'],
                                   keep_default_na=False
        )
        transactions.columns = ['PostingDate', 'Description', 'Amount', 'Check', 'Comment', 'Tags', 'Category']
    else:
        #import credit card transactions
        transactions = pd.read_csv(file_path, parse_dates=['Post Date'], date_parser=chase_date_parser,
                                   usecols=['Post Date', 'Description', 'Amount', 'Comment', 'Tags', 'Category'],
                                   keep_default_na=False
        )
        transactions.columns = ['PostingDate', 'Description', 'Amount', 'Comment', 'Tags', 'Category']
        transactions['Check'] = 0
    transactions['Description'] = transactions['Description'].map(lambda x: " ".join(str(x).split()).replace("'",""))

    try:
        transactions['TransID'] = transactions['PostingDate'].dt.date.map(str) \
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

def insert_transactions(account, trans_to_insert, logging=False):
    print(f"Uploading transactions... {account}")

    if trans_to_insert is None:
        print("No data to insert.")
        return None

    #connect to database
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute(
        f'''
        SELECT Account.Account FROM Account
        WHERE Account.Account = '{account}';
        '''
    )
    if not cursor.fetchone():
        print(f'{trans.TransID}\n--\'{account}\' is not a valid Account.')
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
        tag_list = []

        cursor.execute(
            f'''
            SELECT Category.Category FROM Category
            WHERE Category.Category = '{trans.Category}';
            '''
        )
        if trans.Category and not (trans.IsParentTrans or cursor.fetchone()):
            if logging:
                print('Invalid Category')
            cnt_invalid_category += 1
            print(f'{trans.TransID}\n--\'{trans.Category}\' is not a valid Category.')
            continue

        if trans.Tags is not None and trans.Tags != "":
            tag_list = trans.Tags.split(":")
            for tag in tag_list:
                cursor.execute(
                    f'''
                    SELECT Tag.ID FROM Tag
                    WHERE Tag.TagName = '{tag}';
                    '''
                )
                if not cursor.fetchone():
                    print(f'Invalid Tag "{tag}" at {trans.TransID}')
                    cnt_invalid_tag += 1
                    invalid_tag = True
                    break
            
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
            # if (sum_subtrans_amount != trans.Amount):
            #     invalid_subtrans = True
        else:
            adj_category = trans.Category

        if (not (invalid_subtrans or invalid_tag)):
            try:
                if logging:
                    print(f"{trans.TransID}, {trans.PostingDate}, {trans.Description}, {trans.Amount}, {trans.Comment}, {check}, {account}, {adj_category}")
                cursor.execute(
                    f'''
                    INSERT INTO Transaction (TransID, TransactionDate, Description, Amount, Comment, Check, Account, Category)
                    VALUES ('{trans.TransID}', '{trans.PostingDate}', '{trans.Description}', {trans.Amount}, '{trans.Comment}', {check}, '{account}', '{adj_category}');
                    '''
                )
                cnt_new += 1

                for category, amount in category_spend_dict.items():
                    cursor.execute(
                        f'''
                        INSERT INTO SubTransaction (ParentTransID, Category, Amount)
                        VALUES ('{trans.TransID}', '{category}', {amount});
                        '''
                    )
                for tag in tag_list:
                    cursor.execute(
                        f'''
                        SELECT Tag.ID
                        FROM Tag
                        WHERE (
                            Tag.TagName = '{tag}'
                        );
                        '''
                    )
                    insert_tag = cursor.fetchone()[0]
                    cursor.execute(
                        f'''
                        INSERT INTO TransactionTag (TransID, TagID)
                        VALUES ('{trans.TransID}', {insert_tag});
                        '''
                    )
                
            except pyodbc.IntegrityError:
                if logging:
                    print('Duplicate')
                cnt_exist += 1
                # print(f'{trans.TransID}\n--Record already exists.')
                continue

    conn.commit()
    print(f"{account} upload completed.")
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

def import_insert_transactions(account, file_name, logging=False):
    trans_data = import_transactions(account, file_name)
    insert_transactions(account, trans_data)

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
