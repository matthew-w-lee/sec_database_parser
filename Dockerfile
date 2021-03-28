FROM ubuntu

RUN apt -y update 
RUN apt -y upgrade 
RUN apt install -y build-essential python3-dev python3-pip virtualenv
RUN apt install -y postgresql-client-common libpq-dev
RUN apt-get update
COPY ./lexpredict_openedgar/requirements/base.txt /opt/base.txt
RUN virtualenv -p /usr/bin/python3 env && ./env/bin/pip install --upgrade pip && ./env/bin/pip install -r /opt/base.txt 

#&& ./env/bin/python -m spacy download en_core_web_sm && ./env/bin/pip install xlrd

COPY . /opt/openedgar
RUN cp /opt/openedgar/lexpredict_openedgar/sample.env /opt/openedgar/lexpredict_openedgar/.env
#RUN ./env/bin/pip uninstall -y iPython
EXPOSE 8888
RUN chmod +x /opt/openedgar/entrypoint.sh
ENTRYPOINT ["/opt/openedgar/entrypoint.sh"]