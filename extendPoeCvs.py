import csv
from difflib import SequenceMatcher
from scrapeUrlTableToCsv import scrape
import pandas as pd
import numpy as np
from dateutil.parser import parse


def similar(a, b):
    ratio = 0.0
    ratio = SequenceMatcher(None, a, b).ratio()
    return ratio


def isMatch(a, b):
    if (similar(a, b) > 0.7 or str(a) in str(b) or str(b) in str(a)):
        return True
    elif ('Phaall' in b and 'Pfaall' in a):
        return True
    else:
        return False


def add_missing_title_info_to_csv():
    url = "https://en.wikipedia.org/wiki/Edgar_Allan_Poe_bibliography"
    scrape(url, "poeMetaData")

    with open('poeMetaData/poeMetaData_1.csv', 'a') as csvfile:
        fieldnames = [
            "Title", "Publication date", "First published in", "Genre", "Notes"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writerow({
            "Title":
            """The Narrative Of Arthur
                         Gordon Pym Of Nantucket""",
            "Publication date":
            'February 1837',
            "Genre":
            'Adventure / Hoax'
        })
        writer.writerow({
            "Title": 'An Enigma',
            "Publication date": 'March 1848',
            "Genre": 'Poetry'
        })
        writer.writerow({
            "Title": 'Marginalia',
            "Publication date": '1846',
            "Genre": 'Essay'
        })
        writer.writerow({
            "Title": 'Morning On The Wissahiccon',
            "Publication date": '1844',
            "Genre": 'Essay'
        })
        writer.writerow({
            "Title": 'The Balloon-Hoax',
            "Publication date": 'April 13, 1844',
            "Genre": 'Hoax'
        })


def write_complete_sentence_csv():
    with open('poeComplete.csv', 'w') as csvoutfile:
        with open('poeMetaData/poeMetaData_1.csv') as csvfile:
            title_reader = csv.DictReader(csvfile)
            fieldnames = [
                'sentence_text', 'sentence_number', 'paragraph_number',
                'title', 'publication_date', 'genre'
            ]
            writer = csv.DictWriter(
                csvoutfile, fieldnames=fieldnames, delimiter='|')
            writer.writeheader()
            # print(title_reader.fieldnames)
            titles = []
            titles2 = []
            title_pairs = []
            for trow in title_reader:
                title = str(trow['Title']).title().replace("\"", "")
                titles.append(title)
                with open('poe_sents.csv') as csvfile2:
                    sentence_reader = csv.DictReader(csvfile2, delimiter='|')
                    # print(title)
                    for row in sentence_reader:
                        title2 = str(row['title']).title()
                        if title2 not in titles2:
                            titles2.append(title2)
                        if isMatch(title, title2):
                            row['title'] = title
                            row['publication_date'] = trow['Publication date']
                            row['genre'] = trow['Genre']
                            writer.writerow(row)
                            pair = (title, title2)
                            if pair not in title_pairs:
                                title_pairs.append(pair)


# write_complete_sentence_csv()
# def order_complete_csv_by_date():
with open('poeCompleteOrderedByDate.csv', 'w') as csvoutfile:
    filename = 'poeComplete.csv'
    df = pd.read_csv(
        filename,
        header=0,
        sep="|",
        dtype={
            0: np.object_,
            1: np.object_,
            2: np.object_,
            3: np.object_,
            4: np.object_,
            5: np.object_
        })
    for idx, row in df.iterrows():
        df.loc[idx, 'publication_date'] = parse(str(row['publication_date']))
        genres = str(row['genre']).split(' / ')
        df.loc[idx, 'genre'] = genres
    values = ['publication_date', 'paragraph_number', 'sentence_number']
    df = df.sort_values(by=values, ascending=True).reset_index(drop=True)
    last_count = 0
    for idx, row in df.iterrows():
        length = len(str(row['sentence_text']))
        add_word_count = str(row['sentence_text']) + " " + str(last_count)
        last_count = last_count + length
        df.loc[idx, 'sentence_text'] = add_word_count
    df.to_csv(csvoutfile, sep='|')
