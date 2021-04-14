# SEC Filing Database and Parser
[view demo (password: matt)](http://161.35.250.95:8889)
## About
This project provides a database and parsers for programatically pulling financial data from filings submitted with the U.S. Securities and Exchange Commission ("SEC").

This project is built on top of OpenEDGAR by LexPredict, a Django framework for building databases from EDGAR that can automate the retrieval and parsing of EDGAR forms. A Jupyter Notebook is used as a user interface, communicating with the Django app via a minimal REST HTTP API.

Although a full git commit history is not available for this project, the commit immediately before the latest commit is the OpenEDGAR code base currently available at https://github.com/LexPredict/openedgar. This should provide a diff in the commit history that shows all of the changes and additions made to the OpenEDGAR code in connection with the work on this project.

## Built With
* [OpenEDGAR by LexPredict](https://github.com/LexPredict/openedgar)
* [Jupyter Notebook](https://github.com/jupyter/notebook)
## Getting Started
#### Prerequisites
* Docker
* Docker Compose
#### Installation
* Clone the repository

```bash
    git clone https://github.com/matthew-w-lee/sec_database_parser.git
```
* Enter repository directory

```bash
    cd sec_database_parser
```
* Enter docker-compose up command. It may take a few minutes to download the images.
When the containers boot up, take note of the token provided in logging info from the Jupyter Notebook container.
You'll need it to access the interface.

```bash
    docker-compose up
```
* Run the migrate script using the docker exec command below to do an inital migration of the database.
The container for Django app should be named sec_database_parser_openedgar_1 as set forth below.

```bash
    docker exec -it sec_database_parser_openedgar_1 /bin/bash /opt/openedgar/lexpredict_openedgar/openedgar/migrate.sh
```
* To seed the database with example companies and filings, enter Django's shell by running the shell.sh script with the docker exec command below.

```bash
    docker exec -it sec_database_parser_openedgar_1 /bin/bash /opt/openedgar/lexpredict_openedgar/openedgar/shell.sh
```

* In the shell, run the following command to seed the database. This will download all of the 10-K form filings
for 10 companies.

```python
    exec(open('/opt/openedgar/lexpredict_openedgar/openedgar/seed_db.py').read())
```

* Open the Jupyter Notebook web app in your browser which should be available at localhost on port 8888.
* Enter the token mentioned above seen during container startup. 
* Click on the interface_notebooks folder.
* Click on the notebook_interface.ipynb file to open the interface.

## Usage

#### Access
If locally installed, please follow instructions above. If using demo, enter the password: "matt". Then click on the interface_notebooks folder and click on the notebook_interface.ipynb file.

#### Background
The primary aim of this project is to parse filings submitted with the SEC into a format more useful for analysis by computer. Companies over the years have filed their disclosures in one of several electronic formats -- plain text files (prior to the early 2000s), HTML (early 2000s - present), and XBRL (eXtensible Business Reporting Language) (since the early 2010s).

Although the filings submitted in XBRL offer a clear path towards obtaining such data, the filings made in plain text or HTML are more idiosyncratic and resistant to simple parsing. The organization and format of the information embedded in these filings can vary widely between different companies and different time periods.

#### Django Database
The OpenEDGAR framework is used to download files representing submissions in an organized manner and to store metadata regarding submissions in a PostgresSQL database. 

From there, upon request, this project processes the content of a filing into a data structure that is used as a standard across all filings. The data structure is a List of Dicts, with each Dict representing a "row" of text in the filing along with some other metadata regarding that row that is determined during processing. This conversion helps to standardize a filing's contents with others and to facilitate a search that mimics the "reading" of the filings content on a row-by-row (or paragraph-by-paragraph) basis.

The Django app provides 3 endpoints with the following purposes:
1. getting a list of companies contained in the database; 
2. getting a list of filings for a certain company in the database by CIK (unique id number of a company used by the SEC), and 
3. searching the tables of financial data in a filing; searches can be performed for a specific financial statement type or by matching provided search terms

#### Jupyter Notebook Interface

The Jupyter Notebook interface is composed of 2 parts:
1. A client and methods for requesting and consuming api requests with the Django Database
2. A parser for cleaning and manipulating the table data obtained from the database into a more useful form.

Please see notebook for further instructions on use.

<!-- CONTACT -->
## Contact

Matthew W. Lee - matthew.w.lee44@gmail.com

Project Link: [https://github.com/matthew-w-lee/sec_database_parser](https://github.com/matthew-w-lee/sec_database_parser)
