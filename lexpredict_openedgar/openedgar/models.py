"""
MIT License

Copyright (c) 2018 ContraxSuite, LLC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

# Package imports
import datetime
import django.db.models
from django.db.models import Q
import os
import logging
import pathlib 
from openedgar.clients.local import LocalClient
import openedgar.parsers.edgar
from openedgar.parsers.sec_filing_content_parser import SECFilingContentParser

from pathlib import Path

import pandas
import csv
import json
import re
import requests
import itertools


# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)

DOCUMENT_PATH = os.environ["DOWNLOAD_PATH"]
from openedgar.references import balance_sheet_terms, income_statement_terms, cash_flow_statement_terms, equity_statement_terms, comprehensive_income_terms

class Company(django.db.models.Model):
    """
    Company, which stores a CIK/security company info.
    """

    # Key fields
    cik = django.db.models.BigIntegerField(db_index=True, primary_key=True)
    last_name = django.db.models.CharField(max_length=1024, db_index=True)
    sic = django.db.models.CharField(max_length=1024, db_index=True)

    def __str__(self):
        """
        String representation method
        :return:
        """
        return "Company cik={0}, last_name={1}" \
            .format(self.cik, self.last_name) \
            .encode("utf-8", "ignore") \
            .decode("utf-8", "ignore")

    def get_current_name(self):
        return self.companyinfo_set.order_by('-date')[0].name

    def get_current_sic(self):
        return self.companyinfo_set.order_by('-date')[0].sic

    def latest_info(self):
        return self.companyinfo_set.order('-date')[0]

    def get_10k_num(self):
        return self.get_10ks().count()

    def get_10ks(self):
        return self.filing_set.filter(is_processed=True).filter(form_type__contains = "10-")

class CompanyInfo(django.db.models.Model):
    """
    Company info, which stores a name, SIC, and other data associated with
    a CIK/security on a given date.
    """
    # Fields
    company = django.db.models.ForeignKey(Company, db_index=True, on_delete=django.db.models.CASCADE)
    name = django.db.models.CharField(max_length=1024, db_index=True)
    sic = django.db.models.CharField(max_length=1024, db_index=True, null=True)
    state_location = django.db.models.CharField(max_length=32, db_index=True, null=True)
    state_incorporation = django.db.models.CharField(max_length=32, db_index=True, null=True)
    business_address = django.db.models.CharField(max_length=1024, null=True)
    date = django.db.models.DateField(default=django.utils.timezone.now, db_index=True)

    def cik(self):
        return self.company.cik

    def __str__(self):
        """
        String representation method
        :return:
        """
        return "CompanyInfo cik={0}, name={1}, date={2}" \
            .format(self.company.cik, self.name, self.date) \
            .encode("utf-8", "ignore") \
            .decode("utf-8", "ignore")


class FilingIndex(django.db.models.Model):
    """
    Filing index, which stores links to forms grouped
    by various dimensions such as form type or CIK.
    """

    # Key fields
    edgar_url = django.db.models.CharField(max_length=1024, primary_key=True)
    date_published = django.db.models.DateField(db_index=True, null=True)
    date_downloaded = django.db.models.DateField(default=django.utils.timezone.now, db_index=True)
    total_record_count = django.db.models.IntegerField(default=0)
    bad_record_count = django.db.models.IntegerField(default=0)
    is_processed = django.db.models.BooleanField(default=False, db_index=True)
    is_error = django.db.models.BooleanField(default=False, db_index=True)

    def __str__(self):
        """
        String representation method
        :return:
        """
        return "FilingIndex edgar_url={0}, date_published={1}" \
            .format(self.edgar_url, self.date_published) \
            .encode("utf-8", "ignore") \
            .decode("utf-8", "ignore")


class Filing(django.db.models.Model):
    """
    Filing, which stores a single filing record from an index.
    """

    # Key fields
    form_type = django.db.models.CharField(max_length=64, db_index=True, null=True)
    accession_number = django.db.models.CharField(max_length=1024, null=True)
    date_filed = django.db.models.DateField(db_index=True, null=True)
    company = django.db.models.ForeignKey(Company, db_index=True, on_delete=django.db.models.CASCADE, null=True)
    sha1 = django.db.models.CharField(max_length=1024, db_index=True, null=True)
    s3_path = django.db.models.CharField(max_length=1024, db_index=True)
    document_count = django.db.models.IntegerField(default=0)
    is_processed = django.db.models.BooleanField(default=False, db_index=True)
    is_error = django.db.models.BooleanField(default=False, db_index=True)
    notes = django.db.models.TextField(default=False, db_index=True, null=True)

    def __str__(self):
        """
        String representation method
        :return:
        """
        return "Filing id={0}, cik={1}, form_type={2}, date_filed={3}" \
            .format(self.id, self.company.cik if self.company else None, self.form_type, self.date_filed) \
            .encode("utf-8", "ignore") \
            .decode("utf-8", "ignore")

    def get_buffer(self, filing_path):
        logger.info("Retrieving buffer from S3...")
        client = LocalClient()
        filing_buffer = client.get_buffer(filing_path)
        return openedgar.parsers.edgar.parse_filing(filing_buffer)

    def is_text_file(self):
        return self.first_document().is_text_file()

    def first_document(self):
        return self.filingdocument_set.filter(sequence="1").first()
    
    def exhibit13(self):
        return self.filingdocument_set.filter(Q(description="EX-13") | Q(type="EX-13") | Q(type="EX-13.0")).first()
 
    def raw_content(self):
        raw = self.document_content().content_string
        return {"type": "raw_content", "results": raw}

    def document_content(self):
        ex13 = self.exhibit13()
        if ex13:
            return ex13.document_content()
        else:
            return self.first_document().document_content()

    def search_document(self, search_terms):
        doc = self.document_content()
        found_lines = doc.search(search_terms, how="all", row_type="tables")
        return doc.search(search_terms, how="all", row_type ="tables")

    def financial_statements(self, statement_type):
        if statement_type=="balance_sheet":
            terms = balance_sheet_terms
        if statement_type=="income_statement":
            terms = income_statement_terms
        if statement_type=="comprehensive_income_statement":
            terms = comprehensive_income_terms
        if statement_type=="cash_flow_statement":
            terms = cash_flow_statement_terms
        if statement_type=="equity_statement":
            terms = equity_statement_terms
        doc = self.document_content()
        return doc.biggest_table_on_page(terms, page_lines=range(0,9))


class DocumentContent():

# This class represents the information content of a filing and provides a standardized format
# for searching and obtaining information from a filing.

# Filings are initially parsed into a List of Dicts, with each element in the list representing
# a row of text in the filing. 

    def __init__(self, content_string, is_text_file):
        self.content_string = content_string
        # The following converts the string content of a filing into dict for each row of text from a filing
        # For HTML files, each line reprsents an HTML tag that is a block tag; all inline tags children of a block tag 
        # are concatenated and included as a part of the block tag line; HTML tables are parsed into a list of dicts
        self.lines = SECFilingContentParser(content_string, is_text_file).parse()

    def all_lines(self):
        return {"type": "lines", "results": self.lines}

    def tables(self):
        # Gather all lines part of a table
        return [t for t in self.lines if t['tag'] == "table"]

    def nonTableRows(self):
        # Gather all non-table lines
        new_rows = []
        for row in lines:
            if row["tag"] != "table" and row["tag"] != "tr" and row["tag"] != "td" and row["tag"] != "th":
                new_rows.append(row)
        return new_rows

    def biggest_table_on_page(self, search_terms, how="any", row_type=None, item_sections=[], page_lines=[], response_type="lines"):
        search_response = self.search(search_terms=search_terms, how=how, row_type=row_type, item_sections=item_sections, page_lines=page_lines,\
            response_type = response_type)
        if search_response:
            results = search_response['results']
            # Gather all of the page numbers from the matched lines
            page_nums = list(set([c['page_number'] for c in results]))
            new_results = []
            # Find biggest table on each page
            for p in page_nums:
                page_lines = [line for line in self.lines if line['page_number'] == p]
                tables = [l for l in page_lines if l['tag'] == "table"]
                if tables:
                    final_table = None
                    for t in tables:
                        if final_table:
                            if len(final_table['table_data']) < len(t['table_data']):
                                final_table = t
                        else:
                            final_table = t
                new_results.append(final_table)
            return {"type": "lines", "results": new_results}


    def search(self, search_terms, how="any", row_type=None, item_sections=[], page_lines=[], response_type="lines"):
        # Only searching lines that are not part of a table
        if row_type == "rows":
            searched_lines = self.nonTableRows()
        # Only searching lines that are part of a table
        elif row_type == "tables":
            searched_lines = self.tables()
        # Search all lines
        else:
            searched_lines = self.lines

        # Search only certain item sections
        if item_sections:
            searched_lines = [line for line in searched_lines if line['item_number'] in item_sections]

        # Search only certain page lines
        if page_lines:
            new_searched_lines = []
            lines_w_content = [line for line in searched_lines if line['content']]
            for page_num, page in itertools.groupby(lines_w_content, key=lambda x: x['page_number']):
                new_searched_lines = new_searched_lines + [l for index, l in enumerate(page) if index in page_lines]
            searched_lines = new_searched_lines

        # Run search
        found = []
        for line in searched_lines:
            if self.execute_search(line, search_terms, how):
                found.append(line)

        # Put together response
        if found:
            final_search_results = {"type": None, "results": []}
            # Delivers all of the lines on the page where there is a match
            if response_type == "page":
                final_search_results["type"] = "page"
                r = []
                page_nums = list(set([c['page_number'] for c in found]))
                for p in page_nums:
                    page_lines = [line for line in self.lines if line['page_number'] == p]
                    r.append(page_lines)
                final_search_results['results'] = r
            # Only delivers the lines that have matched
            else:
                final_search_results["type"] = "lines"
                final_search_results['results'] = found
            return final_search_results

    def execute_search(self, line, terms, how):
        # Search that matches any of the search terms
        if how == "any":
            if line['tag'] == "table":
                for content_line in line['content']:
                    if any([re.search(term, str(content_line), re.IGNORECASE) for term in terms]):
                        return True
            else:
                if any([re.search(term, str(line['content']), re.IGNORECASE) for term in terms]):
                    return True
        # Search that must match all search terms
        else:
            if line['tag'] == "table":
                table_line = " ".join(line['content'])
                if all([re.search(term, table_line, re.IGNORECASE) for term in terms]):
                    return True
            else:
                if all([re.search(term, str(line['content']), re.IGNORECASE) for term in terms]):
                    return True


class FilingDocument(django.db.models.Model):
    """
    Filing document, which corresponds to a <DOCUMENT>...</DOCUMENT> section of a <SEC-DOCUMENT>.
    """

    # Key fields
    filing = django.db.models.ForeignKey(Filing, db_index=True, on_delete=django.db.models.CASCADE)
    type = django.db.models.CharField(max_length=1024, db_index=True, null=True)
    sequence = django.db.models.IntegerField(db_index=True, default=0)
    file_name = django.db.models.CharField(max_length=1024, null=True)
    content_type = django.db.models.CharField(max_length=1024, null=True)
    description = django.db.models.CharField(max_length=1024, null=True)
    sha1 = django.db.models.CharField(max_length=1024, db_index=True)
    start_pos = django.db.models.IntegerField(db_index=True)
    end_pos = django.db.models.IntegerField(db_index=True)
    is_processed = django.db.models.BooleanField(default=False, db_index=True)
    is_error = django.db.models.BooleanField(default=False, db_index=True)

    class Meta:
        unique_together = ('filing', 'sequence')

    def __str__(self):
        """
        String representation method
        :return:
        """
        return "FilingDocument id={0}, filing={1}, sequence={2}" \
            .format(self.id, self.filing, self.sequence) \
            .encode("utf-8", "ignore") \
            .decode("utf-8", "ignore")
    
    def is_text_file(self):
        if self.file_name:
            if self.file_name.endswith(".txt"):
                return True
            else:
                return False
        elif self.content_type == "text/plain":
            return True

    def content(self):
        client = LocalClient()
        filing_buffer = client.get_buffer(self.filing.s3_path)
        return filing_buffer[self.start_pos:self.end_pos]

    def document_content(self):
        return DocumentContent(self.content(), self.is_text_file())

class TableBookmark(django.db.models.Model):

    filing = django.db.models.ForeignKey(Filing, db_index=True, on_delete=django.db.models.CASCADE)
    label = django.db.models.CharField(max_length=64, db_index=True, null=True)
    start_index = django.db.models.IntegerField(db_index=True)
    end_index = django.db.models.IntegerField(db_index=True, null=True)


    def __str__(self):
        """
        String rep
        :return:
        """
        return "TableBookmark id={0}".format(self.id)



class SearchQuery(django.db.models.Model):
    """
    Search query object
    """
    form_type = django.db.models.CharField(max_length=64, db_index=True, null=True)
    date_created = django.db.models.DateTimeField(default=datetime.datetime.now)
    date_completed = django.db.models.DateTimeField(null=True)

    def __str__(self):
        """
        String rep
        :return:
        """
        return "SearchQuery id={0}".format(self.id)


class SearchQueryTerm(django.db.models.Model):
    """
    Search term object
    """
    search_query = django.db.models.ForeignKey(SearchQuery, db_index=True, on_delete=django.db.models.CASCADE)
    term = django.db.models.CharField(max_length=128)

    class Meta:
        unique_together = ('search_query', 'term')

    def __str__(self):
        """
        String rep
        :return:
        """
        return "SearchQueryTerm search_query={0}, term={1}".format(self.search_query, self.term)


class SearchQueryResult(django.db.models.Model):
    """
    Search result object
    """
    search_query = django.db.models.ForeignKey(SearchQuery, db_index=True, on_delete=django.db.models.CASCADE)
    filing_document = django.db.models.ForeignKey(FilingDocument, db_index=True, on_delete=django.db.models.CASCADE)
    term = django.db.models.ForeignKey(SearchQueryTerm, db_index=True, on_delete=django.db.models.CASCADE)
    count = django.db.models.IntegerField(default=0)

    def __str__(self):
        """
        String rep
        :return:
        """
        return "SearchQueryTerm search_query={0}, term={1}".format(self.search_query, self.term)
