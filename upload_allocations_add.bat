@echo off
title Upload Allocations

call activate budget

python code\upload_allocations_add.py

pause