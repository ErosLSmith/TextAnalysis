"""This is a test file for python development"""


from pyspark import SparkConf, SparkContext
import functools
from nltk.tokenize import word_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from nltk import pos_tag
from nltk.corpus import stopwords
import os
import shutil

lemmatizer = WordNetLemmatizer()
stopWords = set(stopwords.words('english'))


def pos_words(sentence):
    tokens = pos_tag(word_tokenize(sentence))
    start = int(tokens[-1][0])
    stop = len(tokens) + start - 1
    pos_tokens = zip(tokens, range(start, stop))
    pos_tokens = map(unpack, pos_tokens)
    return pos_tokens


class PosChecker():
    def isAdjective(wordTuple):
        return (wordTuple[1][0] == 'JJ' and len(wordTuple[0]) > 1)\
            and wordTuple[0] not in stopWords

    def isNoun(wordTuple):
        return (wordTuple[1][0] == 'NN' and len(wordTuple[0]) > 1)\
            and wordTuple[0] not in stopWords

    def isVerb(wordTuple):
        return ('VB' in wordTuple[1][0] and len(wordTuple[0]) > 1)\
            and wordTuple[0] not in stopWords

    def isAdverb(wordTuple):
        return ('RB' in wordTuple[1][0] and len(wordTuple[0]) > 1)\
            and wordTuple[0] not in stopWords


def extractWord(pos, wordTuple):
    pos_dic = {'noun': 'n', 'adjective': 'a', 'verb': 'v', 'adverb': 'r'}
    word = str(lemmatizer.lemmatize(wordTuple[0], pos_dic[pos])).lower()
    return (word, (1, wordTuple[1][1]))


def countWord(word1, word2):
    return (word1[0] + word2[0], word1[1] + word2[1])


def toNpArray(wordCount):
    return wordCount[0] + "|" + str(wordCount[1])


def unpack(l):
    a, b = l
    return (a[0], (a[1], [b]))


conf = SparkConf()
sc = SparkContext()
suffix = '.txt'
files = ['poe_sents']
rdds = []
for file in files:
    filename = file + suffix
    rdds.append(sc.newAPIHadoopFile(
        filename,
        "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
        "org.apache.hadoop.io.LongWritable",
        "org.apache.hadoop.io.Text",
        conf={"textinputformat.record.delimiter": "\n"}).map(lambda l: l[1]))
rdd = rdds[0]
for part_rdd in rdds[1:]:
    rdd.union(part_rdd)
words = rdd.flatMap(pos_words)

pos = ["noun", "adjective", "verb", "adverb"]

cwd = os.getcwd()
default_name = "part-00000"
filesep = "/"

for part in pos:
    funcName = "is" + part.capitalize()
    posFunc = getattr(PosChecker, funcName)
    part_words = words.filter(posFunc)
    extract = functools.partial(extractWord, part)
    part_words = part_words.map(extract)
    word_counts = part_words.reduceByKey(countWord)
    np_counts = word_counts.map(toNpArray)
    dirname = part + "_counts.txt"
    foldername = cwd + filesep + dirname
    if os.path.isdir(foldername):
        shutil.rmtree(foldername)
    np_counts.saveAsTextFile(dirname)
    # rename the file
    newfilename = part + "count.txt"
    orginalfilename = foldername + filesep + default_name
    renamedfilename = foldername + filesep + newfilename
    os.rename(orginalfilename, renamedfilename)


sc.stop()
