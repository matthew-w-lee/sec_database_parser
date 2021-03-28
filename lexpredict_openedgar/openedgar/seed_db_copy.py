from allauth.account.models import EmailAddress, EmailConfirmation
from allauth.socialaccount.models import SocialAccount, SocialApp, SocialToken
from django.contrib.admin.models import LogEntry
from django.contrib.auth.models import Group, Permission
from django.contrib.contenttypes.models import ContentType
from django.contrib.sessions.models import Session
from django.contrib.sites.models import Site
from lexpredict_openedgar.users.models import User
from openedgar.models import Company, CompanyInfo, Filing, FilingDocument, FilingIndex, SearchQuery, SearchQueryResult, SearchQueryTerm, TableBookmark
# Shell Plus Django Imports
from django.core.cache import cache
from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import transaction
from django.db.models import Avg, Case, Count, F, Max, Min, Prefetch, Q, Sum, When, Exists, OuterRef, Subquery
from django.utils import timezone
from django.urls import reverse
import pandas
from openedgar.tasks import process_company_filings
ciks = pandas.read_csv("/opt/openedgar/lexpredict_openedgar/openedgar/ciks.csv", header=None)[0].tolist()
for c in ciks[0:10]:
    process_company_filings("LOCAL", c)
