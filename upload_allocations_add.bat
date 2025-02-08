@echo off
title Upload Allocations

call activate budget

python upload_allocations_add.py

pause