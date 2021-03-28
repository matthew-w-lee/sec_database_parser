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

import urllib.parse
import time
import pathlib
from openedgar.parsers.data_frame_parser import DataFrameParser

   
class HTMLTableParser():
    
    def __init__(self, doc):
        self.doc = doc
    
    def parsed_table_unclean(self):
        return self.table_to_2d_dirty(self.doc)

    def parsed_table(self):
        return self.table_to_2d(self.doc)
    
    def clean_text(self, text):
        keep_chars = "[^A-Za-z0-9,\(\)\$\.\%\"\'/:;=\s]+"
        extra_spaces = "\s+"
        new_text = text.replace('\xa0', ' ').replace('\xA0', ' ').replace('\n',' ').strip()
        new_text = re.sub(extra_spaces, " ", new_text)
        new_text = re.sub(keep_chars, "", new_text)
    #    new_text = re.sub("\(", "-", new_text)
        return new_text

    def table_to_2d(self, table_tag):
        rowspans = []  # track pending rowspans
        rows = table_tag.findall('.//tr')

        # first scan, see how many columns we need
        colcount = 0
        for r, row in enumerate(rows):
            cells = row.xpath('.//td | .//th')
            # count columns (including spanned).
            # add active rowspans from preceding rows
            # we *ignore* the colspan value on the last cell, to prevent
            # creating 'phantom' columns with no actual cells, only extended
            # colspans. This is achieved by hardcoding the last cell width as 1. 
            # a colspan of 0 means “fill until the end” but can really only apply
            # to the last cell; ignore it elsewhere. 
            colcount = max(
                colcount,
                sum(int(c.get('colspan', 1)) or 1 for c in cells[:-1]) + len(cells[-1:]) + len(rowspans))
            # update rowspan bookkeeping; 0 is a span to the bottom. 
            rowspans += [int(c.get('rowspan', 1)) or len(rows) - r for c in cells]
            rowspans = [s - 1 for s in rowspans if s > 1]

        # it doesn't matter if there are still rowspan numbers 'active'; no extra
        # rows to show in the table means the larger than 1 rowspan numbers in the
        # last table row are ignored.

        # build an empty matrix for all possible cells
        table = [[None] * colcount for row in rows]

        # fill matrix from row data
        rowspans = {}  # track pending rowspans, column number mapping to count
        for row, row_elem in enumerate(rows):
            span_offset = 0  # how many columns are skipped due to row and colspans 
            for col, cell in enumerate(row_elem.xpath('.//td | .//th')):
                # adjust for preceding row and colspans
                col += span_offset
                while rowspans.get(col, 0):
                    span_offset += 1
                    col += 1

                # fill table data
                rowspan = rowspans[col] = int(cell.get('rowspan', 1)) or len(rows) - row
                colspan = int(cell.get('colspan', 1)) or colcount - col
                # next column is offset by the colspan
                span_offset += colspan - 1
                value = self.clean_text(cell.text_content())
                for drow, dcol in product(range(rowspan), range(colspan)):
                    try:
                        table[row + drow][col + dcol] = value
                        rowspans[col + dcol] = rowspan
                    except IndexError:
                        # rowspan or colspan outside the confines of the table
                        pass

            # update rowspan bookkeeping
            rowspans = {c: s - 1 for c, s in rowspans.items() if s > 1}
        return table

    def table_to_2d_dirty(self, table_tag):
        rowspans = []  # track pending rowspans
        rows = table_tag.findall('.//tr')

        # first scan, see how many columns we need
        colcount = 0
        for r, row in enumerate(rows):
            cells = row.xpath('.//td | .//th')
            # count columns (including spanned).
            # add active rowspans from preceding rows
            # we *ignore* the colspan value on the last cell, to prevent
            # creating 'phantom' columns with no actual cells, only extended
            # colspans. This is achieved by hardcoding the last cell width as 1. 
            # a colspan of 0 means “fill until the end” but can really only apply
            # to the last cell; ignore it elsewhere. 
            colcount = max(
                colcount,
                sum(int(c.get('colspan', 1)) or 1 for c in cells[:-1]) + len(cells[-1:]) + len(rowspans))
            # update rowspan bookkeeping; 0 is a span to the bottom. 
            rowspans += [int(c.get('rowspan', 1)) or len(rows) - r for c in cells]
            rowspans = [s - 1 for s in rowspans if s > 1]

        # it doesn't matter if there are still rowspan numbers 'active'; no extra
        # rows to show in the table means the larger than 1 rowspan numbers in the
        # last table row are ignored.

        # build an empty matrix for all possible cells
        table = [[None] * colcount for row in rows]

        # fill matrix from row data
        rowspans = {}  # track pending rowspans, column number mapping to count
        for row, row_elem in enumerate(rows):
            span_offset = 0  # how many columns are skipped due to row and colspans 
            for col, cell in enumerate(row_elem.xpath('.//td | .//th')):
                # adjust for preceding row and colspans
                col += span_offset
                while rowspans.get(col, 0):
                    span_offset += 1
                    col += 1

                # fill table data
                rowspan = rowspans[col] = int(cell.get('rowspan', 1)) or len(rows) - row
                colspan = int(cell.get('colspan', 1)) or colcount - col
                # next column is offset by the colspan
                span_offset += colspan - 1
                value = cell.text_content()
                for drow, dcol in product(range(rowspan), range(colspan)):
                    try:
                        table[row + drow][col + dcol] = value
                        rowspans[col + dcol] = rowspan
                    except IndexError:
                        # rowspan or colspan outside the confines of the table
                        pass

            # update rowspan bookkeeping
            rowspans = {c: s - 1 for c, s in rowspans.items() if s > 1}


        return table