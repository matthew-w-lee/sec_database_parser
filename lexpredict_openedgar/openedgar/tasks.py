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

# Libraries
import datetime
import hashlib
import logging
import tempfile
import os
import pathlib
from typing import Iterable, Union
import pandas
from lxml.html import parse
import urllib
import re
import numpy
# Packages
import dateutil.parser
import django.db.utils
from celery import shared_task

# Project
from config.settings.base import S3_DOCUMENT_PATH
from openedgar.clients.s3 import S3Client
from openedgar.clients.local import LocalClient
import openedgar.clients.edgar
import openedgar.parsers.edgar
from openedgar.models import Filing, CompanyInfo, Company, FilingDocument, SearchQuery, SearchQueryTerm, \
    SearchQueryResult, FilingIndex, TableBookmark

# LexNLP imports
import lexnlp.nlp.en.tokens

# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)

CLIENT_TYPE = "LOCAL_CLIENT"
LOCAL_DOCUMENT_PATH = os.environ["DOWNLOAD_PATH"]
DOCUMENT_PATH = ""

if CLIENT_TYPE == "S3":
    client = S3Client()
    DOCUMENT_PATH = S3_DOCUMENT_PATH
else:
    client = LocalClient()
    DOCUMENT_PATH = LOCAL_DOCUMENT_PATH

def process_company_filings(client_type: str, cik: str, store_raw: bool = False, store_text: bool = False):
    """
    Process a filing index from an S3 path or buffer.
    :param file_path: S3 or local path to process; if filing_index_buffer is none, retrieved from here
    :param filing_index_buffer: buffer; if not present, s3_path must be set
    :param form_type_list: optional list of form type to process
    :param store_raw:
    :param store_text:
    :return:
    """

    # Log entry
    logger.info("Processing company cik {0}...".format(cik))

    # Get path to filings folder for cik
    cik_path = openedgar.clients.edgar.get_cik_path(cik)
    links = links_10k(cik)

    if client_type == "S3":
        client = S3Client()
    else:
        client = LocalClient()

    # Iterate through links
    bad_record_count = 0
    for row in links:

        # Cleanup path
        if row.lower().startswith("data/"):
            filing_path = "edgar/{0}".format(row)
        elif row.lower().startswith("edgar/"):
            filing_path = row

        # Check if filing record exists
        try:
            filing = Filing.objects.get(s3_path=filing_path)
            logger.info("Filing record already exists: {0}".format(filing))
        except Filing.MultipleObjectsReturned as e:
            # Create new filing record
            logger.error("Multiple Filing records found for s3_path={0}, skipping...".format(filing_path))
            logger.info("Raw exception: {0}".format(e))
            continue
        except Filing.DoesNotExist as f:
            # Create new filing record
            logger.info("No Filing record found for {0}, creating...".format(filing_path))
            logger.info("Raw exception: {0}".format(f))


            # Check if exists; download and upload to S3 if missing
            if not client.path_exists(filing_path):
                # Download
                try:
                    filing_buffer, _ = openedgar.clients.edgar.get_buffer("/Archives/{0}".format(filing_path))
                except RuntimeError as g:
                    logger.error("Unable to access resource {0} from EDGAR: {1}".format(filing_path, g))
                    bad_record_count += 1
                    create_filing_error(row, filing_path)
                    continue

                # Upload
                client.put_buffer(filing_path, filing_buffer)

                logger.info("Downloaded from EDGAR and uploaded to {}...".format(client_type))
            else:
            # Download
                logger.info("File already stored on {}, retrieving and processing...".format(client_type))
                filing_buffer = client.get_buffer(filing_path)

            # Parse
            filing_result = process_filing(client, filing_path, filing_buffer, store_raw=store_raw, store_text=store_text)
            if filing_result is None:
                logger.error("Unable to process filing.")
                bad_record_count += 1
                create_filing_error(row, filing_path)

def bulk_create_bookmarks(filename, label):
    data_file = pandas.read_csv(filename)
    data_file = data_file.to_dict("records")
    for item in data_file:
        label = label
        start_index = item["table_index"]
        end_index = None
        filing = Filing.objects.get(id=item["id"])
        existing = filing.tablebookmark_set.filter(label=label)
        if existing:
            tb = existing.first()
            tb.start_index = start_index
            tb.end_index = None
            tb.save()
        else:
            filing.tablebookmark_set.create(label = label, start_index=start_index, end_index=end_index)

def bulk_create_bookmarks2(filename):
    data_file = pandas.read_csv(filename).fillna("")
    data_file = data_file.to_dict("records")
    for item in data_file:
        filing = Filing.objects.get(id=item["id"])
        for key, value in item.items():
            if not key == "id":
                if value:
                    if "-" in str(value):
                        split_value = str(value).split("-")
                        start_index = int(split_value[0])
                        end_index = int(split_value[1])
                    else:
                        start_index = int(value)
                        end_index = None
                    existing = filing.tablebookmark_set.filter(label=key)
                    if existing:
                        tb=existing.first()
                        tb.start_index = start_index
                        tb.end_index = end_index
                        tb.save()
    #                    print("existing: ", key, start_index, end_index)
                    else:
    #                    print(key, start_index, end_index)
                        filing.tablebookmark_set.create(label = key, start_index=start_index, end_index=end_index)


def create_bookmarks(filing, file):
    data_file = pandas.read_csv(file)
    data_file = data_file.where(pandas.notnull(data_file), None)
    data_file = data_file.to_dict("records")

    filing = filing
    for item in data_file:
        label = item["label"]
        start_index = item["start_index"]
        end_index = None
        existing = filing.tablebookmark_set.filter(label=label)
        if existing:
            existing.first()
            tb = existing.first()
            tb.start_index = start_index
            tb.end_index = end_index
            tb.save()
        else:
            filing.tablebookmark_set.create(label = label, start_index=start_index, end_index=end_index)

def write_all_comp_diluted_eps():
    comps = Company.objects.filter(cik__in=downloaded_companies())
    fname = "/storage/openedgar_eps.csv"
    done_comps = pandas.read_csv(fname)["cik"]
    comps = comps.exclude(cik__in=done_comps)
    for n in comps:
        print("###############################")
        try:
            data = n.full_company_data()
        except:
            continue
        company = None
        if data is not None and not data.empty:
            company = openedgar.parsers.a.CompanyCSV(data)
        if company:
            cols = pandas.Index(range(1990, 2021))
            cols = cols.insert(0, "sic")
            cols = cols.insert(0, "cik")
            master_df = pandas.DataFrame([], columns = cols)
            new_df = (company.print_stats())
            if new_df is not None and not new_df.empty:
                c_name = new_df['company_name'].iat[0]
                sic = new_df['sic'].iat[0]
                cik = new_df['cik'].iat[0]
                just_values_dates = new_df.sort_values("date")[['date', 'value']]
                horizontal = just_values_dates.T
                horizontal.columns = horizontal.loc["date", :]
                new_df = horizontal.drop("date").rename({"value": c_name})
                new_df.insert(0, "sic", sic)
                new_df.insert(0, "cik", cik)
                master_df = pandas.concat([master_df, new_df])
                if os.path.isfile(fname):
                    print("path exists")
                    master_df.to_csv(fname, mode="a", header=False)
                else:
                    print("path does not exist")
                    master_df.to_csv(fname, mode="w", header=True)


def links_10k(cik):
    url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={0}&type=10-K&dateb=&owner=include&count=100".format(cik)
    print("links url: {0}".format(url))
    parsed = parse(urllib.request.urlopen(url))
    doc = parsed.getroot()
    print("parsed doc root tag: {0}".format(doc.tag))
    links = doc.xpath("//a[@id='documentsbutton']")
    link_strings = []
    for e in links:
        link_strings.append(e.get("href"))
    accession = []
    for l in link_strings:
        new_string = l
        new_string = re.sub(r'-index.htm[l]*', '.txt', new_string)
        new_string = re.sub(r'/Archives/', "", new_string)
        accession.append(new_string)
    return accession

def download_10ks(cik):
    print("downloading 10-k accession numbers")
    accession_nums = links_10k(cik)
    for n in accession_nums:
        print("Creating: {}".format(n))
        create_filing(cik, "10-K", n)

def downloaded_companies():
    companies = [yo.company.cik for yo in Filing.objects.filter(form_type="10-K").filter(is_processed=True).select_related("company")]
    companies = list(set(companies))
    return companies

def download_all():
    ciks = pandas.read_csv("/storage/fidelity_all_stocks_cik_and_ticker.csv")
    unique_jack = downloaded_companies()
    cik_filter = ciks['cik'].isin(unique_jack)
    new_ciks = ciks[~cik_filter]
    for c in new_ciks['cik']:
        try:
            x = Company.objects.get(cik=c).filing_set.filter(form_type="10-K").order_by("-date_filed").first().is_processed
            if not x:
                print("staring download for cik: {}".format(c))
                download_10ks(c)
        except:
            print("staring download for cik: {}".format(c))
            download_10ks(c)

def create_filing(cik, form_type, filing_path):

    row = {
        "CIK": cik,
        "Form Type": form_type,
        "File Name": filing_path,
        "Company Name": "ERROR",
        "Date Filed": "19000101"
    }
    # Check if exists; download and upload to S3 if missing
    if not client.path_exists(filing_path):
        # Download
        try:
            filing_buffer, _ = openedgar.clients.edgar.get_buffer("/Archives/{0}".format(filing_path))
        except RuntimeError as g:
            logger.error("Unable to access resource {0} from EDGAR: {1}".format(filing_path, g))
            create_filing_error(row, filing_path)
        # Upload
        client.put_buffer(filing_path, filing_buffer)
        logger.info("Downloaded from EDGAR and uploaded to {}...".format(CLIENT_TYPE))
    else:
        # Download
        logger.info("File already stored on {}, retrieving and processing...".format(CLIENT_TYPE))
        filing_buffer = client.get_buffer(filing_path)

    filing_result = process_filing(client, filing_path, filing_buffer, store_raw=False, store_text=False)
    if filing_result is None:
        logger.error("Unable to process filing.")
        create_filing_error(row, filing_path)

def uploading_text_in_filing_documents(store_raw: False, store_text: True):

    client=LocalClient()
    processed_filings = Filing.objects.filter(is_processed=True)

    for filing in processed_filings:
        buffer_data = client.get_buffer(filing.s3_path)       
        logger.info("parsing id# {0} s3_path: {1}".format(filing.id, filing.s3_path))
        filing_data = openedgar.parsers.edgar.parse_filing(buffer_data, extract=True)
        filing_documents = filing.filingdocument_set.all()
        logger.info("number of FilingDocument objects calculated: {0}".format(len(filing_documents)) )
        documents_data = filing_data["documents"]
        logger.info("number of documents coming from data stream: {0}".format(len(documents_data)) )

        # Iterate through documents
        for document in filing_documents:
            logger.info("WE'RE IN!!!!!!!!!!!!!!!!!!!!!")
            filing_data = None

            for d in documents_data:
                logger.info("documents_data sequence: {0} type: {1}".format(d["sequence"], type(d["sequence"])))
                logger.info("FilingDocument sequence: {0} type: {1}".format(document.sequence,type(document.sequence)))
                if int(d["sequence"]) == document.sequence:
                    logger.info("YAY")
                    filing_data = d
            if filing_data is not None:
           # Upload text to S3 if requested
                if store_text and filing_data["content_text"] is not None:
                    raw_path = pathlib.Path(DOCUMENT_PATH, "text", filing_data["sha1"]).as_posix()
                    if not client.path_exists(raw_path):
                        client.put_buffer(raw_path, filing_data["content_text"], write_bytes=False)
                        logger.info("Uploaded text contents for filing={0}, sequence={1}, sha1={2}"
                                    .format(filing, filing_data["sequence"], filing_data["sha1"]))
                    else:
                        logger.info("Text contents for filing={0}, sequence={1}, sha1={2} already exists on S3"
                                    .format(filing, filing_data["sequence"], filing_data["sha1"]))
            else:
                document.is_processed = False
                document.is_error = True
                document.save()

def create_filing_documents(client, documents, filing, store_raw: bool = False, store_text: bool = False):
    """
    Create filing document records given a list of documents
    and a filing record.
    :param documents: list of documents from parse_filing
    :param filing: Filing record
    :param store_raw: whether to store raw contents
    :param store_text: whether to store text contents
    :return:
    """
    # Iterate through documents
    document_records = []
    for document in documents:
        # Create DB object
        filing_doc = FilingDocument()
        filing_doc.filing = filing
        filing_doc.type = document["type"]
        filing_doc.sequence = document["sequence"]
        filing_doc.file_name = document["file_name"]
        filing_doc.content_type = document["content_type"]
        filing_doc.description = document["description"]
        filing_doc.sha1 = document["sha1"]
        filing_doc.start_pos = document["start_pos"]
        filing_doc.end_pos = document["end_pos"]
        filing_doc.is_processed = True
        filing_doc.is_error = len(document["content"]) > 0
        document_records.append(filing_doc)

        # Upload raw if requested
        if store_raw and len(document["content"]) > 0:
            raw_path = pathlib.Path(DOCUMENT_PATH, "raw", document["sha1"]).as_posix()
            if not client.path_exists(raw_path):
                client.put_buffer(raw_path, document["content"])
                logger.info("Uploaded raw file for filing={0}, sequence={1}, sha1={2}"
                            .format(filing, document["sequence"], document["sha1"]))
            else:
                logger.info("Raw file for filing={0}, sequence={1}, sha1={2} already exists on S3"
                            .format(filing, document["sequence"], document["sha1"]))

        # Upload text to S3 if requested
        if store_text and document["content_text"] is not None:
            raw_path = pathlib.Path(DOCUMENT_PATH, "text", document["sha1"]).as_posix()
            if not client.path_exists(raw_path):
                client.put_buffer(raw_path, document["content_text"], write_bytes=False)
                logger.info("Uploaded text contents for filing={0}, sequence={1}, sha1={2}"
                            .format(filing, document["sequence"], document["sha1"]))
            else:
                logger.info("Text contents for filing={0}, sequence={1}, sha1={2} already exists on S3"
                            .format(filing, document["sequence"], document["sha1"]))

    # Create in bulk
    FilingDocument.objects.bulk_create(document_records)
    return len(document_records)

def create_filing_error(row, filing_path: str):
    """
    Create a Filing error record from an index row.
    :param row:
    :param filing_path:
    :return:
    """
    # Get vars
    cik = row["CIK"]
    company_name = row["Company Name"]
    form_type = row["Form Type"]

    try:
        date_filed = dateutil.parser.parse(str(row["Date Filed"])).date()
    except ValueError:
        date_filed = None
    except IndexError:
        date_filed = None

    # Create empty error filing record
    filing = Filing()
    filing.form_type = form_type
    filing.date_filed = date_filed
    filing.s3_path = filing_path
    filing.is_error = True
    filing.is_processed = False

    # Get company info
    try:
        company = Company.objects.get(cik=cik)

        try:
            _ = CompanyInfo.objects.get(company=company, date=date_filed)
        except CompanyInfo.DoesNotExist:
#        except:
            # Create company info record
            company_info = CompanyInfo()
            company_info.company = company
            company_info.name = company_name
            company_info.sic = None
            company_info.state_incorporation = None
            company_info.state_location = None
            company_info.date = date_filed
            company_info.save()
    except Company.DoesNotExist:
        # Create company
        company = Company()
        company.cik = cik

        try:
            company.save()
        except django.db.utils.IntegrityError:
            return create_filing_error(row, filing_path)

        # Create company info record
        company_info = CompanyInfo()
        company_info.company = company
        company_info.name = company_name
        company_info.sic = None
        company_info.state_incorporation = None
        company_info.state_location = None
        company_info.date = date_filed
        company_info.save()

    # Finally update company and save
    filing.company = company
    filing.save()
    return True

def create_light_filing(row, filing_path: str):
    """
    Create a Filing record from an index row without downloading text file.
    :param row:
    :param filing_path:
    :return:
    """
    # Get vars
    cik = row["CIK"]
    company_name = row["Company Name"]
    form_type = row["Form Type"]

    try:
        date_filed = dateutil.parser.parse(str(row["Date Filed"])).date()
    except ValueError:
        date_filed = None
    except IndexError:
        date_filed = None

    # Create empty error filing record
    filing = Filing()
    filing.form_type = form_type
    filing.date_filed = date_filed
    filing.s3_path = filing_path
    filing.is_error = False
    filing.is_processed = False

    # Get company info
    try:
        company = Company.objects.get(cik=cik)

        try:
            _ = CompanyInfo.objects.get(company=company, date=date_filed)
        except CompanyInfo.DoesNotExist:
            # Create company info record
            company_info = CompanyInfo()
            company_info.company = company
            company_info.name = company_name
            company_info.sic = None
            company_info.state_incorporation = None
            company_info.state_location = None
            company_info.date = date_filed
            company_info.save()
    except Company.DoesNotExist:
        # Create company
        company = Company()
        company.cik = cik

        try:
            company.save()
        except django.db.utils.IntegrityError:
            return create_light_filing(row, filing_path)

        # Create company info record
        company_info = CompanyInfo()
        company_info.company = company
        company_info.name = company_name
        company_info.sic = None
        company_info.state_incorporation = None
        company_info.state_location = None
        company_info.date = date_filed
        company_info.save()

    # Finally update company and save
    filing.company = company
    filing.save()
    return True

@shared_task
def process_filing_index(client_type: str, file_path: str, filing_index_buffer: Union[str, bytes] = None,
                         form_type_list: Iterable[str] = None, store_raw: bool = False, store_text: bool = False):
    """
    Process a filing index from an S3 path or buffer.
    :param file_path: S3 or local path to process; if filing_index_buffer is none, retrieved from here
    :param filing_index_buffer: buffer; if not present, s3_path must be set
    :param form_type_list: optional list of form type to process
    :param store_raw:
    :param store_text:
    :return:
    """
    # Log entry
    logger.info("Processing filing index {0}...".format(file_path))

    if client_type == "S3":
        client = S3Client()
    else:
        client = LocalClient()

    # Retrieve buffer if not passed
    if filing_index_buffer is None:
        logger.info("Retrieving filing index buffer for: {}...".format(file_path))
        filing_index_buffer = client.get_buffer(file_path)

    # Write to disk to handle headaches
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(filing_index_buffer)
    temp_file.close()

    # Get main filing data structure
    filing_index_data = openedgar.parsers.edgar.parse_index_file(temp_file.name)
    logger.info("Parsed {0} records from index".format(filing_index_data.shape[0]))

    # Iterate through rows
    bad_record_count = 0
    for _, row in filing_index_data.iterrows():
        # Check for form type whitelist
        if form_type_list is not None:
            if row["Form Type"] not in form_type_list:
                logger.info("Skipping filing {0} with form type {1}...".format(row["File Name"], row["Form Type"]))
                continue

        # Cleanup path
        if row["File Name"].lower().startswith("data/"):
            filing_path = "edgar/{0}".format(row["File Name"])
        elif row["File Name"].lower().startswith("edgar/"):
            filing_path = row["File Name"]

        # Check if filing record exists
        try:
            filing = Filing.objects.get(s3_path=filing_path)
            logger.info("Filing record already exists: {0}".format(filing))
        except Filing.MultipleObjectsReturned as e:
            # Create new filing record
            logger.error("Multiple Filing records found for s3_path={0}, skipping...".format(filing_path))
            logger.info("Raw exception: {0}".format(e))
            continue
        except Filing.DoesNotExist as f:
            # Create new filing record
            logger.info("No Filing record found for {0}, creating...".format(filing_path))
            logger.info("Raw exception: {0}".format(f))
            create_light_filing(row, filing_path)
    # Create a filing index record
    edgar_url = "//{0}".format(file_path).replace("//", "/")
    try:
        filing_index = FilingIndex.objects.get(edgar_url=edgar_url)
        filing_index.total_record_count = filing_index_data.shape[0]
        filing_index.bad_record_count = bad_record_count
        filing_index.is_processed = True
        filing_index.is_error = False
        filing_index.save()
        logger.info("Updated existing filing index record.")
    except FilingIndex.DoesNotExist:
        filing_index = FilingIndex()
        filing_index.edgar_url = edgar_url
        filing_index.date_published = None
        filing_index.date_downloaded = datetime.date.today()
        filing_index.total_record_count = filing_index_data.shape[0]
        filing_index.bad_record_count = bad_record_count
        filing_index.is_processed = True
        filing_index.is_error = False
        filing_index.save()
        logger.info("Created new filing index record.")

    # Delete file if we make it this far
    os.remove(temp_file.name)


@shared_task
def process_filing(client, file_path: str, filing_buffer: Union[str, bytes] = None, store_raw: bool = False,
                   store_text: bool = False):
    """
    Process a filing from a path or filing buffer.
    :param file_path: path to process; if filing_buffer is none, retrieved from here
    :param filing_buffer: buffer; if not present, s3_path must be set
    :param store_raw:
    :param store_text:
    :return:
    """
    # Log entry
    logger.info("Processing filing {0}...".format(file_path))


    # Check for existing record first
    try:
        filing = Filing.objects.get(s3_path=file_path)
        if filing is not None:
            logger.error("Filing {0} has already been created in record {1}".format(file_path, filing))
            return None
    except Filing.DoesNotExist:
        logger.info("No existing record found.")
    except Filing.MultipleObjectsReturned:
        logger.error("Multiple existing record found.")
        return None

    # Get buffer
    if filing_buffer is None:
        logger.info("Retrieving filing buffer from S3...")
        filing_buffer = client.get_buffer(file_path)

    # Get main filing data structure
    filing_data = openedgar.parsers.edgar.parse_filing(filing_buffer, extract=store_text)
    if filing_data["cik"] is None:
        logger.error("Unable to parse CIK from filing {0}; assuming broken and halting...".format(file_path))
        return None

    try:
        # Get company
        company = Company.objects.get(cik=filing_data["cik"])
        logger.info("Found existing company record.")

        # Check if record exists for date
        try:
            _ = CompanyInfo.objects.get(company=company, date=filing_data["date_filed"])

            logger.info("Found existing company info record.")
        except CompanyInfo.DoesNotExist:
            # Create company info record
            company_info = CompanyInfo()
            company_info.company = company
            company_info.name = filing_data["company_name"]
            company_info.sic = filing_data["sic"]
            company_info.state_incorporation = filing_data["state_incorporation"]
            company_info.state_location = filing_data["state_location"]
            company_info.date = filing_data["date_filed"].date() if isinstance(filing_data["date_filed"],
                                                                               datetime.datetime) else \
                filing_data["date_filed"]
            company_info.save()

            logger.info("Created new company info record.")

    except Company.DoesNotExist:
        # Create company
        company = Company()
        company.cik = filing_data["cik"]

        try:
            # Catch race with another task/thread
            company.save()

            try:
                _ = CompanyInfo.objects.get(company=company, date=filing_data["date_filed"])
            except CompanyInfo.DoesNotExist:
                # Create company info record
                company_info = CompanyInfo()
                company_info.company = company
                company_info.name = filing_data["company_name"]
                company_info.sic = filing_data["sic"]
                company_info.state_incorporation = filing_data["state_incorporation"]
                company_info.state_location = filing_data["state_location"]
                company_info.date = filing_data["date_filed"]
                company_info.save()
        except django.db.utils.IntegrityError:
            company = Company.objects.get(cik=filing_data["cik"])

        logger.info("Created company and company info records.")

    # Now create the filing record
    try:
        filing = Filing()
        filing.form_type = filing_data["form_type"]
        filing.accession_number = filing_data["accession_number"]
        filing.date_filed = filing_data["date_filed"]
        filing.document_count = filing_data["document_count"]
        filing.company = company
        filing.sha1 = hashlib.sha1(filing_buffer).hexdigest()
        filing.s3_path = file_path
        filing.is_processed = False
        filing.is_error = True
        filing.save()
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Unable to create filing record: {0}".format(e))
        return None

    # Create filing document records
    try:
        create_filing_documents(client, filing_data["documents"], filing, store_raw=store_raw, store_text=store_text)
        filing.is_processed = True
        filing.is_error = False
        filing.save()
        return filing
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Unable to create filing documents for {0}: {1}".format(filing, e))
        return None


@shared_task
def extract_filing(client, file_path: str, filing_buffer: Union[str, bytes] = None):
    """
    Extract the contents of a filing from an S3 path or filing buffer.
    :param file_path: S3 path to process; if filing_buffer is none, retrieved from here
    :param filing_buffer: buffer; if not present, s3_path must be set
    :return:
    """
    # Get buffer



    if filing_buffer is None:
        logger.info("Retrieving filing buffer from S3...")
        filing_buffer = client.get_buffer(file_path)

    # Get main filing data structure
    _ = openedgar.parsers.edgar.parse_filing(filing_buffer)


@shared_task
def search_filing_document_sha1(client, sha1: str, term_list: Iterable[str], search_query_id: int, document_id: int,
                                case_sensitive: bool = False,
                                token_search: bool = False, stem_search: bool = False):
    """
    Search a filing document by sha1 hash.
    :param stem_search:
    :param token_search:
    :param sha1: sha1 hash of document to search
    :param term_list: list of terms
    :param search_query_id:
    :param document_id:
    :param case_sensitive:
    :return:
    """
    # Get buffer
    logger.info("Retrieving buffer from S3...")
    text_s3_path = pathlib.Path(DOCUMENT_PATH, "text", sha1).as_posix()
    document_buffer = client.get_buffer(text_s3_path).decode("utf-8")

    # Check if case
    if not case_sensitive:
        document_buffer = document_buffer.lower()

    # TODO: Refactor search types
    # TODO: Cleanup flow for reduced recalc
    # TODO: Don't search same SHA1 repeatedly, but need to coordinate with calling process

    # Get contents
    if not token_search and not stem_search:
        document_contents = document_buffer
    elif token_search:
        document_contents = lexnlp.nlp.en.tokens.get_token_list(document_buffer)
    elif stem_search:
        document_contents = lexnlp.nlp.en.tokens.get_stem_list(document_buffer)

    # For term in term list
    counts = {}
    for term in term_list:
        # term_tokens = lexnlp.nlp.en.tokens.get_token_list(term)

        if stem_search:
            term = lexnlp.nlp.en.tokens.DEFAULT_STEMMER.stem(term)

        if case_sensitive:
            counts[term] = document_contents.count(term)
        else:
            counts[term] = document_contents.count(term.lower())

    search_query = None
    results = []
    for term in counts:
        if counts[term] > 0:
            # Get search query if empty
            if search_query is None:
                search_query = SearchQuery.objects.get(id=search_query_id)

            # Get term
            search_term = SearchQueryTerm.objects.get(search_query_id=search_query_id, term=term)

            # Create result
            result = SearchQueryResult()
            result.search_query = search_query
            result.filing_document_id = document_id
            result.term = search_term
            result.count = counts[term]
            results.append(result)

    # Create if any
    if len(results) > 0:
        SearchQueryResult.objects.bulk_create(results)
    logger.info("Found {0} search terms in document sha1={1}".format(len(results), sha1))
    return True


@shared_task
def extract_filing_document_data_sha1(client, sha1: str):
    """
    Extract structured data from a filing document by sha1 hash, e.g.,
    dates, money, noun phrases.
    :param sha1:
    :param document_id:
    :return:
    """
    # Get buffer
    logger.info("Retrieving buffer from S3...")
    text_s3_path = pathlib.Path(DOCUMENT_PATH, "text", sha1).as_posix()
    document_buffer = client.get_buffer(text_s3_path).decode("utf-8")

    # TODO: Build your own database here.
    _ = len(document_buffer)
