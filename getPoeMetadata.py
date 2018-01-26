from bs4 import BeautifulSoup
import requests
import re
import csv
from to_sents import split_into_sentences as getSentences


def cleanhtml(raw_html, regx=re.compile('<.*?>')):
    cleantext = re.sub(regx, '', raw_html)
    return cleantext


url = "https://books.eserver.org/fiction/poe/predicament"

resp = requests.get(url)
# print(resp)
soup = BeautifulSoup(resp.content, "lxml")
# print(soup.prettify())
pattern = re.compile("^/fiction/poe/")
base_url = "https://books.eserver.org"
atags = soup.findAll('a', {'href': pattern})

with open('poe_sents.csv', 'w') as csvfile:
    fieldnames = ['sentence_text', 'sentence_number', 'paragraph_number',
                  'title']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter='|')
    writer.writeheader()

    for a in atags:
        partial_url = a['href']
        url = base_url + partial_url
        # print(url)
        resp = requests.get(url)
        soup = BeautifulSoup(resp.content, "lxml")
        title = soup.body.main.h1
        title = cleanhtml(str(title))
        title = str(title).replace("Edgar Allan Poe: ", "")
        ptags = soup.div.findAll("p")

        for i in range(len(ptags)):
            para = cleanhtml(str(ptags[i]))
            sents = getSentences(para)
            for j in range(len(sents)):
                sent_text = sents[j]
                if len(sent_text) > 1:
                    writer.writerow({'sentence_text': sent_text,
                                     'sentence_number': j,
                                     'paragraph_number': i,
                                     'title': title})
