#!/bin/bash
cd /opt/openedgar/lexpredict_openedgar
source /env/bin/activate
source .env
python manage.py runserver 0.0.0.0:8000