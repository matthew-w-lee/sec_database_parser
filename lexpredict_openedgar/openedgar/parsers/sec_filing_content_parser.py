import lxml
from lxml.html import parse
from lxml.html import fromstring
from lxml.html.clean import Cleaner
from lxml import etree
import re
import itertools
import pandas
import numpy
from openedgar.parsers.html_table_parser import HTMLTableParser
from openedgar.parsers.text_table_parser import TextTableParser

class SECFilingContentParser():

    def __init__(self, raw_content, is_text_file):
        self.is_text_file = is_text_file
        if is_text_file:
            self.raw_content = raw_content.decode("ascii").splitlines()
            self.lines = self.process_text()
        else:
            self.raw_content = raw_content.decode("ascii")
#            self.elements = self.all_html_elements()
            self.lines = self.process_html()

    def parse(self):
        return self.lines

    def inline(self): 
        return ["a","abbr","acronym","b","bdo","big","button","cite","code","dfn","em", "font", "i",\
        "img","input", "kbd","label","map","object","output","q","samp","script","select","small",\
            "span", "strong", "sub", "sup", "textarea", "time", "tt","var"]

    def all_html_elements(self):
        cleaner = Cleaner(comments=True, safe_attrs=lxml.html.defs.safe_attrs | set(['style'])) 
        html_string = cleaner.clean_html(self.raw_content)
        doc = fromstring(html_string)
        return list(doc.iter())

    def extract_tables_from_html(self):
        cleaner = Cleaner(comments=True, safe_attrs=lxml.html.defs.safe_attrs | set(['style'])) 
        html_string = cleaner.clean_html(self.raw_content)
        doc = fromstring(html_string)
        tables = {}
        for i, table in enumerate(doc.iter("table")):
            parent = table.getparent()
            index = parent.index(table)
            parent.insert(index, etree.XML("<table name='{0}'></table>".format(i)))
            tables[str(i)] = table
            parent.remove(table)
        return [doc, tables]

    def process_text(self):
        final_lines = []
        style=None
        item_number=None
        element_position = None
        page_number = 1
        table_index = 1
        table_index_place_holder = None
        for i, e in enumerate(self.raw_content):
            item_num_search = re.search("^item\s+[0-9]+[A-Za-z]*", e.strip(), re.IGNORECASE)
            if item_num_search:
                numbers = re.search("[0-9]+[A-Za-z]*", e.strip(), re.IGNORECASE)
                item_number = numbers.group(0)
            page_num_search = re.search("<page>", e.strip(), re.IGNORECASE)
            if page_num_search:
                page_number = page_number + 1
            table_start_index_search = re.search("[<]table[>]", e.strip(), re.IGNORECASE)
            if table_start_index_search:
                table_index_place_holder = table_index
            table_end_index_search = re.search("[<][/]table[>]", e.strip(), re.IGNORECASE)
            if table_end_index_search:
                table_index_place_holder = None
                table_index += 1
            line_data = {"line_index": i, "element_index": element_position, "item_number": item_number, "page_number": page_number,\
                "tag": None, "style": None, "content": e, "table_index": table_index_place_holder, "table_data": None}
            final_lines.append(line_data)
        table_dict = {}
        for index, table in itertools.groupby(final_lines, key=lambda x: x['table_index']):
            if index:
                content = [tab['content'] for tab in list(table)]
                try:
                    table_dict[index] = {}
                    table_dict[index]["table_data"] = TextTableParser(content).parse_table()
                    table_dict[index]["content"] = content    
                except:
                    table_dict[index] = {}
                    table_dict[index]["table_data"] = content    
                    table_dict[index]["content"] = content
        table_indicies = list(set([l['table_index'] for l in final_lines if l['table_index']]))
        for ti in table_indicies:
            first_line = [l for l in final_lines if l['table_index'] == ti][0]
            index = final_lines.index(first_line)
            new_line = first_line
            new_line['tag'] = "table"
            new_line['content'] = table_dict[new_line['table_index']]['content']
            new_line['table_data'] = table_dict[new_line['table_index']]['table_data']
            for l in final_lines:
                if l['table_index'] == ti:
                    final_lines.remove(l)
            final_lines.insert(index, new_line)
#        final_df = pandas.DataFrame(final_lines)
#        final_df = final_df.replace(numpy.nan, False, regex=True)
#        final_df = final_df.drop_duplicates(subset="table_index")
#        final_lines = final_df.to_dict("records")
        #get a set of all the table indexes; then iterate over them, insert one table line before the first of each; then remove the rest
 #       for line in final_lines:
 #           if line['table_index']:
 #               line['tag'] = "table"
 #               line['content'] = table_dict[line['table_index']]["content"]
 #               line['table_data'] = table_dict[line['table_index']]['table_data']
        return final_lines

    def test_process_html(self):
        html_doc, tables = self.extract_tables_from_html()
        lines = []
#        text = ""
#        style= ""
#        item_number = None
#        element_position=0
#        page_number = 1
        for i, e in enumerate(html_doc.iter()):
            if not e.tag in self.inline():
                print(etree.tostring(e, encoding='unicode', method='text', pretty_print=True))


    def process_html(self):
        html_doc, tables = self.extract_tables_from_html()
        lines = []
        text = ""
        style= ""
        item_number = None
        element_position=0
        page_number = 1
        for i, e in enumerate(html_doc.iter()):
            if not e.tag in self.inline():
                element_position=i
                search = re.search("^\s*item\s*[0-9]+[A-Za-z]*", text, re.IGNORECASE)
                if search:
                    numbers = re.search("[0-9]+[A-Za-z]*", text, re.IGNORECASE)
                    item_number = numbers.group(0)
                if e.tag == "table":
                    table_index = e.get("name")
                    table_doc = tables[table_index]
                    table_data = HTMLTableParser(table_doc).parsed_table_unclean()
                    table_text = self.extract_table_content(table_doc)
                    line_data = {"line_index": len(lines), "element_index": element_position, "item_number": item_number, "page_number": page_number,\
                        "tag": e.tag, "style": style, "content": table_text, "table_index": table_index, "table_data": table_data}
                else:
                    line_data = {"line_index": len(lines), "element_index": element_position, "item_number": item_number, "page_number": page_number,\
                    "tag": e.tag, "style": style, "content": text.strip(), "table_index": None, "table_data": None}
                lines.append(line_data)
                text = ""
                style = ""
            if e.text:
                text = text + e.text.replace(u'\xa0',' ')
            if e.tail:
                text = text + e.tail.replace(u'\xa0',' ')
            if e.get("style"):
                style = style + e.get("style").strip() + ";"
            if e.tag == "hr":
                page_number = page_number + 1
            elif e.tag == "div" and e.get("style"):
                if "page-break-" in e.get("style"):
                    page_number = page_number + 1                    
        return lines


    def extract_table_content(self, table_doc):
        content = []
        text = ""
        for element in table_doc.iter():
#            print(element.text)
            if not element.tag in self.inline():
                content.append(text.strip())
                text = ""
            if element.text:
                text = text + element.text
            if element.tail:
                text = text + element.tail
        return content

    def old_process_html(self):
        lines = []
        text = ""
        style= ""
        item_number = None
        element_position=0
        page_number = 1
        table_index = None
        table_data = None
        for i, e in enumerate(self.elements):
            if not e.tag in self.inline():
                element_position=i
                search = re.search("^\s*item\s*[0-9]+[A-Za-z]*", text, re.IGNORECASE)
                if search:
                    numbers = re.search("[0-9]+[A-Za-z]*", text, re.IGNORECASE)
                    item_number = numbers.group(0)
                if e.tag == "table":
                    if table_index is None:
                        table_index = 0
                    else:
                        table_index += 1
                    table_data = HTMLTableParser(e).parsed_table_unclean()
                    line_data = {"line_index": len(lines), "element_index": element_position, "item_number": item_number, "page_number": page_number,\
                        "tag": e.tag, "style": style, "content": text.strip(), "table_index": table_index, "table_data": table_data}
                elif e.tag == "tr" or e.tag=="td" or e.tag=="th":
                    line_data = {"line_index": len(lines), "element_index": element_position, "item_number": item_number, "page_number": page_number,\
                        "tag": e.tag, "style": style, "content": text.strip(), "table_index": table_index, "table_data": None}
                else:
                    line_data = {"line_index": len(lines), "element_index": element_position, "item_number": item_number, "page_number": page_number,\
                    "tag": e.tag, "style": style, "content": text.strip(), "table_index": None, "table_data": None}
                lines.append(line_data)
                text = ""
                style = ""
            if e.text:
                text = text + e.text.replace(u'\xa0',' ')
            if e.tail:
                text = text + e.tail.replace(u'\xa0',' ')
            if e.get("style"):
                style = style + e.get("style").strip() + ";"
            if e.tag == "hr":
                page_number = page_number + 1
            elif e.tag == "div" and e.get("style"):
                if "page-break-" in e.get("style"):
                    page_number = page_number + 1                    
        return lines
