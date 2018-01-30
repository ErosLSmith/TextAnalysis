"""This is a Spark application that takes either a or a text file or csv file
    and outputs four files with the counts of nouns, adjectives, adverbs, and
    verbs.
    The header format for input csv file:
    sentence_text|sentence_number|paragraph_number|title
    The format for the input text file:
    One sentence per line of the file
    In both formats the sentences are followed by a cumlative word count. This
    word count is required for processing by this application.
    """

import os
import shutil
import functools
import atexit
from glob import glob

from nltk.tokenize import word_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from nltk import pos_tag
from nltk.corpus import stopwords

import pandas as pd

from pyspark import SparkContext
from pyspark import rdd
from pyspark.sql import SQLContext
from pyspark.sql import Row


def exit_handler():
    """Ensure SparkContext is stopped before exit"""
    sc.stop()
    print("exiting")


# Setting up globals and spark context
atexit.register(exit_handler)
sc = SparkContext()

lemmatizer = WordNetLemmatizer()
stopWords = set(stopwords.words('english'))


def pos_words_meta(data: str or Row, isRow: bool = None) -> map:
    """Take a sentence with a trailing integer and tokenize all it's words into
     their parts of speech using the nltk tokenizer.

    Args:
        Row: Takes a Row with a full sentence with a trailing interger value.
        That value is the position of beginning word relative to the entire
        text of which the sentence was extracted.

    Returns:
        A list of nltk tokens tupled with that token's position in sentence
        offset by the trailing integer value resulting in a token coupled with
        it's word position relative to the entire text body.

    Raises:
        AssertionError: Need to know datatype of input
        TypeError: If input is not indexable type with correct keys
        TypeError: If sentence is not parseable by word_tokenize
        ValueError: If last token of sentence is not convertable to an integer
    """
    sentence_text = ""
    sentence_number = None
    if isRow is None:
        raise AssertionError("Need a boolean value in second argument.")

    pos_tokens = []
    args = None
    out_tokens = None
    if isRow is True:
        sentence_text, sentence_number = extract_from_row(data)
        tokens, start, stop, length = tokenize_sentence(sentence_text)
        s = [sentence_number for i in range(length)]
        args = (tokens, range(start, stop), s)
    else:
        sentence_text = data
        tokens, start, stop, length = tokenize_sentence(sentence_text)
        args = (tokens, range(start, stop))
    pos_tokens = zip(*args)
    out_tokens = map(unpack_meta, pos_tokens)
    return out_tokens


def extract_from_row(data: Row) -> (str, int):
    try:
        sentence_text = data['sentence_text']
        sentence_number = data['Unnamed: 0']
    except TypeError:
        raise TypeError("Input error, Wasn't able to get data from Row")
    return sentence_text, sentence_number


def tokenize_sentence(sentence: str) -> (list, int, int, int):
    """Take a sentence with a trailing integer and tokenize all it's words into
     their parts of speech using the nltk tokenizer.

    Args:
        sentence: String of a sentence with a trailing interger value.
        That value is the position of beginning word relative to the entire
        text of which the sentence was extracted.

    Returns:
        list: A list of nltk tokens.
        int: Start of offset to get position of each word
        int: Stop last word position in sentence
        int: Length of list of tokens.

    Raises:
        TypeError: If sentence is not parseable by word_tokenize
        ValueError: If last token of sentence is not convertable to an integer
    """
    try:
        tokens = pos_tag(word_tokenize(sentence))
    except TypeError:
        raise TypeError("Sentence argument parsing error")
    try:
        start = int(tokens[-1][0])
    except ValueError:
        raise ValueError("Last token '" + tokens[-1][0] +
                         "' was not convertable to an integer.")
    stop = len(tokens) + start - 1
    length = len(range(start, stop))
    return tokens, start, stop, length


class PosChecker():
    """
    PosChecker is a container for boolean checking methods on the part of
    speech of a wordTuple returned from the word_tokenize nltk package.
    """

    def isAdjective(wordTuple: tuple) -> bool:
        """
        Take a word tuple from the nltk tokenizer and return whether it is
        an adjective

        Args:
            wordTuple: A tuple with a word and a code for its part of speech.

        Returns:
            bool: True if word is an adjective. False otherwise
        """
        return (wordTuple[1][0] == 'JJ' and len(wordTuple[0]) > 1)\
            and wordTuple[0] not in stopWords

    def isNoun(wordTuple):
        """
        Take a word tuple from the nltk tokenizer and return whether it is
        a noun.

        Args:
            wordTuple: A tuple with a word and a code for its part of speech.

        Returns:
            bool: True if word is a noun. False otherwise
        """
        return (wordTuple[1][0] == 'NN' and len(wordTuple[0]) > 1)\
            and wordTuple[0] not in stopWords

    def isVerb(wordTuple):
        """
        Take a word tuple from the nltk tokenizer and return whether it is
        a verb.

        Args:
            wordTuple: A tuple with a word and a code for its part of speech.

        Returns:
            bool: True if word is a verb. False otherwise
        """
        return ('VB' in wordTuple[1][0] and len(wordTuple[0]) > 1)\
            and wordTuple[0] not in stopWords

    def isAdverb(wordTuple):
        """
        Take a word tuple from the nltk tokenizer and return whether it is
        an adverb.

        Args:
            wordTuple: A tuple with a word and a code for its part of speech.

        Returns:
            bool: True if word is a adverb. False otherwise
        """
        return ('RB' in wordTuple[1][0] and len(wordTuple[0]) > 1)\
            and wordTuple[0] not in stopWords


def extractWord(pos: str, wordTuple: tuple) -> str:
    """
    Take a word tuple from the nltk tokenizer and the part of speech of the
    word and extract a lemmatized version of the word.

    Args:
        pos: A string with the part of speech of the word i.e. "noun",
        "adjective", "verb", "adverb"
        wordTuple: A tuple with a word and a code for its part of speech.

    Returns:
        str: a lemmatized version of word extracted from tuple
    """
    pos_dic = {'noun': 'n', 'adjective': 'a', 'verb': 'v', 'adverb': 'r'}
    word = str(lemmatizer.lemmatize(wordTuple[0], pos_dic[pos])).lower()
    return (word, (1, wordTuple[1][1]))


def countWord(word1: list, word2: list) -> tuple:
    """
    A fuction used for reduce that sum first two values in two arrays
    into one value and the second two value of the two arrays into a
    second value and returns them as a tuple.

    Args:
        word1: A list with two values that can be added
        word2: A second list with two values that can be added

    Returns:
        tuple: A tuple with first two added values from the lists
    """
    return (word1[0] + word2[0], word1[1] + word2[1])


def toNpArray(wordCount: list, delimiter="|") -> str:
    """
    Stringify map reduce output for a csv file.

    Args:
        wordCount: A tuple with first value as the word and the second
        value as the reduced value or values.
    Returns:
        str: A string to write to the output file.
    """
    return wordCount[0] + delimiter + str(wordCount[1])


def unpack_meta(l: list) -> tuple:
    """
    Unpack and repack value in a tuple for easy processing in map reduce.

    Args:
        list: A list with part of speech and the word as the first value "a"
        and the position of the word as "b" and the position of the sentence
        it was in as c[0]. Addition values not used yet.
    Returns:
        str: A repacked version of the input ready for map-reduce.
    """
    a, b, *c = l
    if c == []:
        return (a[0], (a[1], [b]))
    else:
        return (a[0], (a[1], [(b, *c)]))


def make_count_files_from_csv(file_name: str, delimiter: str = "|"):
    """
    Take in a cvs file with fields for sentence_text using default delimiter
    of "|" and extract the four parts of speech "noun", "adjective", "verb",
    and "adverb" with their counts and placements within the text. Sentences
    should be preprocessed with trailing intergers giving the cumlative word
    counts of the sentence within the origin textual body. The line number
    of file will added for future reference in data processing.

    Args:
        file_name: A file_name for a csv file with recommended format.
    """
    sql_sc = SQLContext(sc)
    pandas_df = pd.read_csv(file_name, delimiter)
    s_df = sql_sc.createDataFrame(pandas_df)
    pos_func = functools.partial(pos_words_meta, isRow=True)
    words = s_df.rdd.flatMap(pos_func)
    map_reduce_sentences_to_pos(words)


def make_count_files():
    """
    Take in a text file with sentence text and extract the four parts of speech
    "noun", "adjective", "verb", and "adverb" with their counts and placements
    within the text. Sentences should be preprocessed with trailing intergers
    giving the cumlative word counts of the sentence within the origin textual
    body.

    Args:
        file_name: A file_name for a text file with recommended format.
    """
    suffix = '.txt'
    files = ['poe_sents']
    rdds = []
    for file in files:
        filename = file + suffix
        rdds.append(
            sc.newAPIHadoopFile(
                filename,
                "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                "org.apache.hadoop.io.LongWritable",
                "org.apache.hadoop.io.Text",
                conf={
                    "textinputformat.record.delimiter": "\n"
                }).map(lambda l: l[1]))
    rdd = rdds[0]
    for part_rdd in rdds[1:]:
        rdd.union(part_rdd)
    pos_func = functools.partial(pos_words_meta, isRow=False)
    words = rdd.flatMap(pos_func)
    map_reduce_sentences_to_pos(words)


def map_reduce_sentences_to_pos(words: rdd.RDD):
    """
    Perform map reduce functions on text rdd extracting parts of speech from
    the text, counting, lemmatizing, and indexing them, writing the final
    results to separate files depending on the part of speech.

    Args:
        words: A RDD with nltk tokenized words and indexing information.
    """

    pos = ["noun", "adjective", "verb", "adverb"]

    cwd = os.getcwd()
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


def write_count_files_from_csv(filename):
    """
    Take in a text file with sentence text and extract the four parts of speech
    "noun", "adjective", "verb", and "adverb" with their counts and placements
    within the text. Sentences should be preprocessed with trailing intergers
    giving the cumlative word counts of the sentence within the origin textual
    body.

    Args:
        file_name: A file_name for a text file with recommended format.
    """
    make_count_files_from_csv(filename)
    pos = ["noun", "adjective", "verb", "adverb"]
    cwd = os.getcwd() + "/"
    for part in pos:
        dirname = cwd + part + "_counts.txt"
        files = glob(dirname + "/part-0000*")
        with open(dirname + "/" + part + "count.txt", 'w') as outfile:
            for fname in files:
                with open(fname) as infile:
                    for line in infile:
                        outfile.write(line)


filename = "poeCompleteOrderedByDate.csv"
write_count_files_from_csv(filename)
sc.stop()
#
# pos_func = functools.partial(pos_words_meta, isRow=False)
# w_map = pos_func("I am a friendly sentence. 0")
# row = Row()
# row = row("sentence_text", "Unnamed: 0")
# row = row("I am a friendly sentence. 0", 0)
# print(row)
# pos_func = functools.partial(pos_words_meta, isRow=True)
# w_map = pos_func(row)

# for k, v in w_map:
#     print((k, v))
