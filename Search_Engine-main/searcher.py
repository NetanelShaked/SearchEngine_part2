from parser_module import Parse
from ranker import Ranker
from myTokenizer import Tokenizer
import utils
import pickle


class Searcher:

    def __init__(self, inverted_index):
        """
        :param inverted_index: dictionary of inverted index
        """
        # self.parser = Parse()
        self.ranker = Ranker()
        self.inverted_index = inverted_index
        # self.tokenizer = Tokenizer(self.parser)

    def relevant_terms_in_inverted_index(self, query):
        """
        this function compare the parsed query to the inverted index
        in order to remove words that not showed in the corpus
        and make fit with the same words that's wrote a little bit differently.
        :param query - parsed query. given in Dictionary.
        :return a Dictionary that's fit the inverted index.
        """
        query_tokens = {}
        for term in query:
            key = term
            if key not in self.inverted_index:
                if key.lower() in self.inverted_index:
                    key = key.lower()
                elif term.upper() in self.inverted_index:
                    key = key.upper()
                else:
                    continue
            query_tokens[key] = query[term]
        return query_tokens

    def relevant_docs_from_posting(self, query):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        relevant_docs = {}
        posting_files = {}
        if len(query) == 0:
            return relevant_docs
        for token in query:
            if token in self.inverted_index:
                posting_files[token] = self.inverted_index[token][3]
        query = sorted(posting_files.keys(), key=lambda x: posting_files[x])
        garage_name = posting_files[query[0]]
        is_file_open = False
        for term in query:
            if term not in self.inverted_index:
                continue
            if garage_name != posting_files[term]:
                garage_name = posting_files[term]
                is_file_open = False
            if not is_file_open:
                file = open(self.inverted_index['__output_path__'] + "/" + str(garage_name) + ".pkl", 'rb')
                data = pickle.load(file)
                file.close()
                is_file_open = True
            twit_id = data[self.inverted_index[term][2]]
            for i in twit_id:
                if i[0] not in relevant_docs:
                    relevant_docs[i[0]] = {term: (i[4], i[5])}
                else:
                    relevant_docs[i[0]][term] = (i[4], i[5])

        return relevant_docs
