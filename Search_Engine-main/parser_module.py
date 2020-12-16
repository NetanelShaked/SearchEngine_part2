import re

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from configuration import ConfigClass
from document import Document
from stemmer import Stemmer
from myTokenizer import Tokenizer


class Parse:

    def __init__(self,inverted_idx):
        self.stop_words = stopwords.words('english')
        dic_stop_words = {}
        for word in self.stop_words:
            dic_stop_words[word] = 1
        self.stop_words = dic_stop_words
        self.__Stemmer = Stemmer()
        self.my_tokenizer = Tokenizer(inverted_idx,self)


    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text: the full text that will be tokenized
        :return: List of Tokens from the text.
        """
        text_tokens = self.my_tokenizer.preProcess(text)
        text_tokens_without_stopwords = [w for w in text_tokens if w.lower() not in self.stop_words]
        self.my_tokenizer.insert_text(text_tokens_without_stopwords)
        text_tokens_without_stopwords = self.my_tokenizer.tokenize()
        if(ConfigClass.get_toStem()):
            text_tokens_with_stemmer = []
            for w in text_tokens_without_stopwords:
                if w.lower() not in self.stop_words and len(w) > 0:
                    if w[0].isupper():
                        text_tokens_with_stemmer.append(self.__Stemmer.stem_term(w).upper())
                    else:
                        text_tokens_with_stemmer.append(self.__Stemmer.stem_term(w).lower())
            text_tokens_without_stopwords = text_tokens_with_stemmer
        else:
            text_tokens_without_stopwords = [w for w in text_tokens_without_stopwords if w.lower() not in self.stop_words]

        return text_tokens_without_stopwords
    def parse_doc(self, doc_as_list):
        """
        This function takes a tweet document as list and break it into different fields
        :param doc_as_list: list re-preseting the tweet.
        :return: Document object with corresponding fields.
        """
        tweet_id = doc_as_list[0]
        tweet_date = doc_as_list[1]
        full_text = doc_as_list[2]
        url = doc_as_list[3]
        retweet_text = doc_as_list[4]
        retweet_url = doc_as_list[5]
        quote_text = doc_as_list[6]
        quote_url = doc_as_list[7]
        term_dict = {}
        tokenized_text = self.parse_sentence(full_text)

        doc_length = len(tokenized_text)  # after text operations.

        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, None, doc_length)

        for term in tokenized_text:
            if len(term) == 0 : continue
            if (term[0] != '#' and term[0] != '@') and (len(term.split(' ')) > 1 or len(term.split('-')) > 1):
                document.addEntite(term)
                continue
            else:
                if term not in term_dict:
                    term_dict[term] = 1
                else:
                    term_dict[term] += 1

        tokenized_url = self.parse_sentence(url)
        for term in tokenized_url:
            if len(term) == 0 : continue
            if term not in term_dict:
                term_dict[term] = 1
            else:
                term_dict[term] += 1

        max_tf = 0

        for value in term_dict.values():
            if value > max_tf:
                max_tf = value

        document.setTermDic(term_dict)
        document.set_max_tf(max_tf)
        document.set_unique_words_amount(len(term_dict)+len(document.entites))

        return document
