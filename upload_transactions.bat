@echo off
title Upload Transactions

call "C:\ProgramData\Anaconda3\condabin\activate.bat" budget

python "C:\Users\James\GitRepos\Budget\upload_checking.py"

python "C:\Users\James\GitRepos\Budget\upload_savings.py"

python "C:\Users\James\GitRepos\Budget\upload_creditcard.py"

pause