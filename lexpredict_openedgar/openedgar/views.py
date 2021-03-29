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

from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.parsers import JSONParser
from rest_framework.pagination import PageNumberPagination
from rest_framework import viewsets, status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import Company, Filing
from .serializers import CompanySerializer, FilingSerializer
import openedgar.tasks
import json
import lxml
import lxml.html
import requests


@csrf_exempt
@api_view(["GET"])
def companies(request, format=None):
    data = []
    companies = Company.objects.all()
    for c in companies:
        latest_info = c.companyinfo_set.order_by('-date')[0]
        c_data = {"date": latest_info.date, "cik": c.cik, "name": latest_info.name, "sic": latest_info.sic}
        data.append(c_data)
    return Response(data)


@api_view(["GET"])
def company_detail(request, pk, format=None):
    try:
        company = Company.objects.get(cik=pk)
        filings = company.filing_set.all()
    except Company.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)
    data = []
    for f in filings:
        row = {
            "id": f.id,
            "accession_number": f.accession_number,
            "date_filed": f.date_filed,
            "form_type": f.form_type,
            "is_error": f.is_error,
            "is_processed": f.is_processed,
            "notes": f.notes
        }
        data.append(row)
    return Response(data)

@api_view(["GET"])
def single_filing(request, cik, pk, format=None):
    try:
        filing = Filing.objects.get(pk=pk)
    except Filing.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)
    if request.body:
        search_words = json.loads(request.body)
        if search_words["request_type"] == "financial_statements":
            statement_type = search_words["statement_type"]
            data = filing.financial_statements(statement_type)
            return Response(data)
        else:
            terms = search_words["search_terms"]
            data = filing.search_document(terms)
            return Response(data)
    else:
        # Return HTML or text of filing
        data = filing.raw_content()
        return Response(data)


@api_view(["GET"])
def search_tables(request, cik, pk, format=None):
    try:
        filing = Filing.objects.get(pk=pk)
    except Filing.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)
    data = filing.tables()
    return Response(data)
