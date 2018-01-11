"""This is a test file for python development"""


from pyspark import SparkConf, SparkContext
from nltk.tokenize import word_tokenize
from nltk import pos_tag
from nltk.corpus import stopwords
# import csv
# import io


stopWords = set(stopwords.words('english'))
len(stopWords)


def pos_words(sentence):
    tokens = pos_tag(word_tokenize(sentence))
    start = int(tokens[-1][0])
    stop = len(tokens) + start - 1
    pos_tokens = zip(tokens, range(start, stop))
    pos_tokens = map(unpack, pos_tokens)
    return pos_tokens


def isAdjective(wordTuple):
    return (wordTuple[1][0] == 'JJ' and len(wordTuple[0]) > 1)\
        and wordTuple[0] not in stopWords


def isNoun(wordTuple):
    return (wordTuple[1][0] == 'NN' and len(wordTuple[0]) > 1)\
        and wordTuple[0] not in stopWords


def isVerb(wordTuple):
    return ('VB' in wordTuple[1][0] and len(wordTuple[0]) > 1)\
        and wordTuple[0] not in stopWords


def extractWord(wordTuple):
    return (wordTuple[0], (1, wordTuple[1][1]))


def countWord(word1, word2):
    return (word1[0] + word2[0], word1[1] + word2[1])


def toNpArray(wordCount):
    return wordCount[0] + "|" + str(wordCount[1])


def unpack(l):
    a, b = l
    return (a[0], (a[1], [b]))


# print(countWord((1, [8]), (1, [23])))

s = "this line is a warm sentence. 4"


# pos_tokens = pos_words(s)
# for t in pos_tokens:
#     print(t)
#     if isAdjective(t):
#         print(extractWord(t))
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
print(rdds[0].first())
rdd = rdds[0]
for part_rdd in rdds[1:]:
    rdd.union(part_rdd)
nouns = rdd.flatMap(pos_words)
print(nouns.first())
nouns = nouns.filter(isNoun)
# # print(nouns.first())
nouns = nouns.map(extractWord)
# print(nouns.first())
noun_counts = nouns.reduceByKey(countWord)
# print(noun_counts.first())
noun_np = noun_counts.map(toNpArray)
# f = noun_np.first()
# print(f)

# verbs = rdd.flatMap(pos_words).filter(isVerb).map(extractWord)
# verb_counts = verbs.reduceByKey(countWord)
# verb_np = verb_counts.map(toNpArray)
# adjectives = rdd.flatMap(pos_words).filter(isAdjective).map(extractWord)
# adjective_counts = adjectives.reduceByKey(countWord)
# adjective_np = adjective_counts.map(toNpArray)

noun_np.saveAsTextFile("noun_counts.txt")
# adjective_np.saveAsTextFile("adjective_counts.txt")
# verb_np.saveAsTextFile("verb_counts.txt")

sc.stop()
