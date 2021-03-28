from rest_framework import serializers
from .models import Company, Filing, CompanyInfo

class FilingSerializer(serializers.ModelSerializer):
    class Meta:
        model = Filing
        fields = ['pk', 'form_type', 'accession_number', 'date_filed', 'is_processed', 'is_error']

class CompanySerializer(serializers.ModelSerializer):

    class Meta:
        model = Company
        fields = ['pk', 'cik', "company_infos"]
        depth = 1

class CompanyInfo(serializers.ModelSerializer):
    
    class Meta:
        model = CompanyInfo
        fields = ["name", "sic", "date"]