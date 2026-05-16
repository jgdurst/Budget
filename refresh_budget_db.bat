@echo off
title Upload Allocations

call activate budget

python code\refresh_budget_db.py

pause