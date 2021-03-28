
import pandas
import numpy
from pandas import DataFrame
from pandas import Series
from lxml.html import parse
from lxml.html import fromstring
from lxml.html.clean import Cleaner
from lxml import etree
import urllib
import os
import re
import requests
#import openedgar.clients.edgar
#import openedgar.parsers.edgar
from io import BytesIO
#import spacy
#nlp = spacy.load("en_core_web_sm")
import re
from requests import get
from datetime import datetime

class DataFrameParser():
    
    def __init__(self, df, axis=0):
        self.df = df
        self.axis=axis
                
    def get_all_same_indicies(self):
        all_same_bool = self.df.apply(lambda x: self.is_all_same(x), axis=self.axis)
        return [i for i in all_same_bool.index if all_same_bool[i]]

    def get_only_dollar_paren_indicies(self):
        dollar_paren_bool = self.df.apply(lambda x: self.only_contains_dollar_or_paren(x), axis=self.axis)
        return [i for i in dollar_paren_bool.index if not dollar_paren_bool[i]]
    
    def get_row_indicies_after_word(self, search_word, num_of_rows_after):
        i = self.df.str.contains(search_word, na=False, case=False, regex=True).idxmax()
        return self.df.loc[i:i+num_of_rows_after].index

    def extract_year(self):
        new_columns = self.df.apply(lambda x: self.find_year(x))
        return new_columns    
    
    def get_account_names(self):
        return self.df.apply(lambda x: self.process_account_name(x), axis=self.axis)
    
    def blacklisted_df(self):
        return self.df.applymap(lambda x: remove_blacklisted(x))
    
    def character_count_df(self):
        return self.blacklisted_df().applymap(lambda x: len(str(x)))

    def find_year(self, cell):
        year_regex = "[12][90][9012][0-9]"
        year_match = re.search(year_regex, str(cell))
        if year_match:
            return year_match.group(0)

    def process_account_name(self, old_series):
        series = old_series.replace('^\s*$', numpy.nan, regex=True, inplace=False).astype("object")
        if series.isna().all():
            return ""
        else:
            new_series = series.dropna().values
            all_same = self.is_all_same(new_series)
            if all_same:
                return new_series[0]
            else:
                new_word = ""
#                print(len(new_series))
                for ind, x in enumerate(new_series):
                    if ind < len(new_series)-1:
                        if new_series[ind] == new_series[ind + 1]:
                            new_word = new_series[ind]
                        else:
                            new_word = "{0} {1}".format(new_series[ind], new_series[ind+1])
                return new_word
    
    def is_all_same(self,series):
        a = series
        return (a[0] == a).all()

    def only_contains_dollar_or_paren(self, series):
        new_series = series.dropna()
        return new_series.str.contains("^[\s$)%]+$", na=False, case=False, regex=True).all()

