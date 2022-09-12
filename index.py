from elasticsearch import Elasticsearch
from elasticsearch import helpers
import io
import os
import json
import datetime
import time
import re
import gzip
import os
import xml.etree.cElementTree as ET
# C implementation of ElementTree


from elasticsearch import Elasticsearch

es = Elasticsearch(
        cloud_id="uihealth-i-o-optimized-deployment:Y2VudHJhbHVzLmF6dXJlLmVsYXN0aWMtY2xvdWQuY29tOjkyNDMkMGI3NjI1YTZkMGZiNDQ1ZDhjODc1Y2E5NDkxNzkzZWMkNTE0NjljOWFlZDRkNDc5MmFlYzdkMGJiZTk0YjlmMjU=",
        http_auth=("samhita", "sprintern21"))  # Replace with your username and password


def create_pubmed_paper_index():
    settings = {
        # changing the number of shards after the fact is not
        # possible max Gb per shard should be 30Gb, replicas can
        # be produced anytime

        "number_of_shards": 5,
        "number_of_replicas": 0
    }
    mappings = {

            "properties" : {
                "title": { "type": "text", "analyzer": "english"},
                "abstract": { "type": "text", "analyzer": "english"},
                "date": {
                    "type":   "date",
                    "format": "yyyy-MM-dd"

                },
                "url": {
                    "type": "keyword"
                }

            }

    }
    es.indices.delete(index="pubmed", ignore=[400, 404])
    es.indices.create(index="pubmed",
                       body={'settings': settings,
                              'mappings': mappings},
                       request_timeout=30)
    return


def get_es_docs(paper):
    source = {
        'title': paper.title,
        'date': paper.created_datetime.date(),
        'abstract': paper.abstract,
        "url": "https://pubmed.ncbi.nlm.nih.gov/" + paper.pm_id
    }
    doc = {
        "index": {
            "_index": "pubmed",
            "_id": paper.pm_id

        }
    }
    return doc, source


class Pubmed_paper():
    ''' Used to temporarily store a pubmed paper outside es '''
    def __init__(self):
        self.pm_id = 0
        # every paper has a created_date
        self.created_datetime = datetime.datetime.today()
        self.title = ""
        self.abstract = ""

    def __repr__(self):
        return '<Pubmed_paper %r>' % (self.pm_id)


the_path_of_files = ["/Users/samhita/Downloads/pubmed21n1071.xml.gz"]

es = Elasticsearch(
        cloud_id="uihealth-i-o-optimized-deployment:Y2VudHJhbHVzLmF6dXJlLmVsYXN0aWMtY2xvdWQuY29tOjkyNDMkMGI3NjI1YTZkMGZiNDQ1ZDhjODc1Y2E5NDkxNzkzZWMkNTE0NjljOWFlZDRkNDc5MmFlYzdkMGJiZTk0YjlmMjU=",
        http_auth=("samhita", "sprintern21"))  # Replace with your username and password


def extract_data(citation):
    new_pubmed_paper = Pubmed_paper()

    citation = citation.find('MedlineCitation')

    new_pubmed_paper.pm_id = citation.find('PMID').text
    new_pubmed_paper.title = citation.find('Article/ArticleTitle').text

    Abstract = citation.find('Article/Abstract')
    if Abstract is not None:
        # Here we discard information about objectives, design,
        # results and conclusion etc.
        for text in Abstract.findall('AbstractText'):
            if text.text:
                if text.get('Label'):
                    new_pubmed_paper.abstract += '<b>' + text.get('Label') + '</b>: '
                new_pubmed_paper.abstract += text.text + '<br>'

    DateCreated = citation.find('DateRevised')
    new_pubmed_paper.date = datetime.datetime(
        int(DateCreated.find('Year').text),
        int(DateCreated.find('Month').text),
        int(DateCreated.find('Day').text)
    )
    doc, source = get_es_docs(new_pubmed_paper)
    del new_pubmed_paper
    return doc, source


def fill_pubmed_papers_table(the_path_of_files):


    for i, f in enumerate(the_path_of_files):
        print("Read file %d filename = %s" % (i, f))
        time0 = time.time()
        time1 = time.time()
        inF = gzip.open(f, 'rb')
        # we have to iterate through the subtrees, ET.parse() would result
        # in memory issues
        context = ET.iterparse(inF, events=("start", "end"))
        # turn it into an iterator
        context = iter(context)

        # get the root element
        event, root = context.__next__()
        print("Preparing the file: %0.4fsec" % ((time.time() - time1)))
        time1 = time.time()

        documents = []
        time1 = time.time()
        for event, elem in context:
            if event == "end" and elem.tag == "PubmedArticle":
                doc, source = extract_data(elem)
                documents.append(doc)
                documents.append(source)
                elem.clear()
        root.clear()
        print("Extracting the file information: %0.4fsec" %
              (time.time() - time1))
        time1 = time.time()

        res = es.bulk(index="pubmed", body=documents, request_timeout=300)
        print("Indexing data: %0.4fsec" % ((time.time() - time1)))
        print("Total time spend on this file: %0.4fsec\n" %
              (time.time() - time0))
        #os.remove(f) # we directly remove all processed files
    return




def main():

    create_pubmed_paper_index()
    fill_pubmed_papers_table(the_path_of_files)










if __name__ == "__main__":
    main()

