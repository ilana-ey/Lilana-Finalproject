'''
Plan:
1.) request API and get a dictionary or list of 100 days 
between months of may-august 2022
2.) get tempeture, weather conditions: percipation, wind, etc..
3.) put in a csv on github maybe
4.) or json file 
    {date:[temperature, percipatation, wind, extra]...}

Functions to make:
1.) get weather api 
    make a request and import data into dicitonary or file
2.) write to database 
3.) functions to grab from past work
    setupdatabase - hw8
    main 
    write_csv - project 2

'''



# Imports
import os
import requests
import json
import re
import csv 

# Task 1: