class Document:

    def __init__(self, tweet_id, tweet_date=None, full_text=None, url=None, retweet_text=None, retweet_url=None,
                 quote_text=None, quote_url=None, term_doc_dictionary=None, doc_length=0):
        """
        :param tweet_id: tweet id
        :param tweet_date: tweet date
        :param full_text: full text as string from tweet
        :param url: url
        :param retweet_text: retweet text
        :param retweet_url: retweet url
        :param quote_text: quote text
        :param quote_url: quote url
        :param term_doc_dictionary: dictionary of term and documents.
        :param doc_length: doc length
        """
        self.tweet_id = tweet_id
        self.tweet_date = tweet_date
        self.full_text = full_text
        self.url = url
        self.retweet_text = retweet_text
        self.retweet_url = retweet_url
        self.quote_text = quote_text
        self.quote_url = quote_url
        self.term_doc_dictionary = term_doc_dictionary
        self.doc_length = doc_length
        ## add index for each word
        self.max_tf = None
        self.unique_words_amount = 0
        self.entites = {}

    def setTermDic(self, term_dic):
        self.term_doc_dictionary = term_dic

    def addEntite(self, term):
        if (term in self.entites):
            self.entites[term] += 1
        else:
            self.entites[term] = 1

    def getEntiteDic(self):
        return self.entites

    def set_max_tf(self, max_tf):
        self.max_tf = max_tf

    def set_unique_words_amount(self, amount):
        self.unique_words_amount = amount

    def get_unique_words_amount(self):
        return self.unique_words_amount

    def get_max_tf(self):
        return self.max_tf

    def get_twit_id(self):
        return self.tweet_id