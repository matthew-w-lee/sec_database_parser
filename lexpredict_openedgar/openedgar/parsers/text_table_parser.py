# Packages
import pandas
import numpy
from pandas import DataFrame
from pandas import Series

from lxml.html import parse
from lxml.html import fromstring
from lxml.html.clean import Cleaner
from lxml import etree
import re
import urllib
from datetime import datetime
from itertools import product
import itertools
import urllib.parse
import time
import pathlib
from openedgar.parsers.data_frame_parser import DataFrameParser

class TextPageFinder():
  
    
    def find_all_text_table_indicies(self, text):
        indicies=[]
        for i, line in enumerate(text):
            if re.search("<PAGE>", str(line), re.IGNORECASE):
                indicies.append(i)
        return indicies
    
    def page_indicies(self, lines):
        index_set = []
        indicies = self.find_all_text_table_indicies(lines)
        for page_num, i in enumerate(indicies):
            if page_num == 0:
                indy = {"page_number": page_num, "start_index": 0, "end_index": i}
            elif page_num < len(indicies) - 1:
                prior_index = indicies[page_num - 1]
                indy = {"page_number": page_num, "start_index": prior_index, "end_index": i}
            index_set.append(indy)
        return index_set


    def extract_all_pages(self, lines):
        indicies = self.find_all_text_table_indicies(lines)
#        index_sets = list(self.chunks(indicies, 2))
        final_pages = []
        for page_num, i in enumerate(indicies):
            if page_num == 0:
                final_pages.append(lines[0:i])
            elif page_num < len(indicies) - 1:
                prior_index = indicies[page_num - 1]
                final_pages.append(lines[prior_index:i])
        return final_pages

    def chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]


class TextTableFinder():
  
    
    def find_all_text_table_indicies(self, text):
        indicies=[]
        for i, line in enumerate(text):
            if re.search("<TABLE>|</TABLE>", line):
                indicies.append(i)
        return indicies

    def extract_all_text_tables(self, text):
        indicies = self.find_all_text_table_indicies(text)
        if len(indicies) % 2 == 0:
            index_sets = list(self.chunks(indicies, 2))
            final_tables = []
            
            for i in index_sets:
                final_tables.append(text[i[0]:i[1]])
            return final_tables

    def chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

class TextTableParser:
    
    ALL_EXCEPT_NUMBERS_REGEX = "^(^\s*[(]*[0-9,.]+\s*)$"
    YEAR_REGEX = "([12][90][9012][0-9])"

    def __init__(self, array):
        self.text_array = array
        self.column_indicator_row_index = self.column_indicator_index()
        self.column_starts = self.find_column_starts()
        self.first_column_end_pos = self.column_starts[0]

    def clean_text(self, text):
        keep_chars = "[^A-Za-z0-9,\(\)\$\.\%\"\'/:;=\s]+"
        extra_spaces = "\s+"
        new_text = text.replace('\xa0', ' ').replace('\xA0', ' ').replace('\n',' ').strip()
        new_text = re.sub(extra_spaces, " ", new_text)
        new_text = re.sub(keep_chars, "", new_text)
        new_text = new_text.replace("..", "")
        return new_text

    def header_section(self):
        new_table=[]
        main_table = self.text_array[:self.column_indicator_row_index] 
        indicies = self.column_starts
        for line in main_table:
            new_line = []
            for num, index in enumerate(indicies):
                if num == len(indicies)-1:
                    new_line.append(line[indicies[num]:])
                else:
                    new_line.append(line[indicies[num]:indicies[num+1]])
            new_table.append(new_line)
        new_df = DataFrame(new_table)
        new_df = new_df.applymap(lambda x: self.clean_text(str(x)))
        col_names = DataFrameParser(new_df).get_account_names()
        return col_names
    
    def original_accounts_text(self):
        main_table = self.text_array[self.column_indicator_row_index+1:]
        indicies = self.column_starts
        new_table = []
        for line in main_table:
            new_table.append(line[0:indicies[0]])
        return Series(new_table)
    
    def data_and_accounts_section(self):
        new_table = []
        main_table = self.text_array[self.column_indicator_row_index+1:]
        indicies = self.column_starts
        indicies.insert(0, 0)
        for line in main_table:
            new_line=[]
            if len(line) > indicies[-1]:
                for num, index in enumerate(indicies):
                    if num == len(indicies)-1:
                        new_line.append(line[indicies[num]:])
                    else:
                        new_line.append(line[indicies[num]:indicies[num+1]])
            else:
                for num, index in enumerate(indicies):
                    if num==0:
                        new_line.append(line)
                    else:
                        new_line.append("")
            new_table.append(new_line)
        new_df = DataFrame(new_table)
        new_df = new_df.applymap(lambda x: self.clean_text(str(x)))
        return new_df

    def old_parse_table(self, only_year=False):
        header = self.header_section()
        d_and_a = self.data_and_accounts_section()
        if header is not None and not header.empty:
            final_table = d_and_a.iloc[:, 1:]
            final_cols = header
            if only_year:
                if final_cols.str.contains(self.YEAR_REGEX, regex=True, na=False, case=False).all():
                    final_cols = final_cols.str.extract(self.YEAR_REGEX, expand=False)
            final_table.columns = final_cols
            accounts = d_and_a.iloc[:, 0].str.replace("\s*[.]\s*$", "", regex=True)
            final_table.insert(0, "account", accounts)
            return final_table.values.tolist()


    def parse_table(self, only_year=False):
        df = pandas.DataFrame(self.text_array)
        starts = self.column_starts
        for i, s in enumerate(starts):
            if i < len(starts) - 1:
                df[i+1] = df[0].str.slice(start=s, stop=starts[i+1])
            else:
                df[i+1] = df[0].str.slice(start=s)
        df = df.iloc[:, 1:]
        indicator_row = self.column_indicator_row_index
        columns = df.iloc[:indicator_row, :]
        rows_with_markup = columns.apply(lambda x: x.str.contains("[<>]", regex=True, na=False, case=False), axis=1).any(axis=1)
        columns = columns.loc[~rows_with_markup, :]
        rows = df.iloc[indicator_row+1:, :]
        if columns.empty:
            return rows.values.tolist()
        else:
            df = pandas.concat([columns, rows])
            rows_with_letters = df.apply(lambda x: x.str.contains("[a-z0-9]", regex=True, na=False, case=False), axis=1).any(axis=1)
            df = df[rows_with_letters]
            return df.values.tolist()

    def find_column_indicator_row_index(self):
        return self.text_array.index(self.column_indicator_row())

    def column_indicator_index(self):
        for i, line in enumerate(self.text_array):
            if re.search("<S>", line):
                return i


    def column_indicator_row(self):
        for line in self.text_array:
            if re.search("<S>", line):
                return line
    
    def find_column_starts(self):
        indicator_row = str(self.text_array[self.column_indicator_row_index])
        return [m.start() for m in re.finditer("<", indicator_row)]
    
    def subsection(self, df, subset_search_column=None, subset_search_word=None, subset_rows_after=None):
        if subset_search_column and subset_search_word and subset_rows_after:
            sub_set_indicies = DataFrameParser(df.loc[:, subset_search_column]).get_row_indicies_after_word(subset_search_word, subset_rows_after)
            return df.loc[sub_set_indicies, :]
