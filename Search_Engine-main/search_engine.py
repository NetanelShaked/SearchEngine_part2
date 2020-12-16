import os
import utils
from configuration import ConfigClass
from indexer import Indexer
from parser_module import Parse
from reader import ReadFile
from searcher import Searcher
import time
import threading
import pandas as pn


def indexer_multiprocess(indexer, document_list):
    for parsed_document in document_list:
        indexer.add_new_doc(parsed_document)


def run_engine(output_path, corpus_path=None, stemming=False):
    """
    this function goes over the parquests file that given in corpus_path, parsing and indexing
    every single document in the parquests files
    :param output_path - at this path the posting files will be saved. given in String.
    :param corpus_path - the path that all the parquests files will be located. given in String.
    :param stemming - will determine if using stemmer on this corpus or not. given in Boolean.
    :return: None.
    """

    if not os.path.isdir(output_path):
        os.mkdir(output_path)
        if stemming == True:
            output_path += '/WithStem'
        else:
            output_path += '/WithoutStem'
        os.mkdir(output_path)
    config = ConfigClass()
    if corpus_path is None:
        r = ReadFile(
            corpus_path=ConfigClass.get__corpusPath())
    else:
        r = ReadFile(corpus_path=corpus_path)
    ConfigClass.set_stem(stemming)
    indexer = Indexer(config, 300000, output_path, 15 * 10 ** 6)
    p = Parse(indexer.get_inv_idx())
    document_parsed_list = []
    thread_is_on = False
    document_parsed_counter = 0
    thread = None
    counter = 1
    for documents_list in r:
        if document_parsed_counter % 100000 == 0:
            indexer.empty_pendling()
        counter += 1
        for document in documents_list:
            # parse the document
            document_parsed_list.append(p.parse_doc(document))
            document_parsed_counter += 1
            if document_parsed_counter % 50000 == 0:
                if thread_is_on:
                    thread.join()
                    thread_is_on = False
                thread = threading.Thread(target=indexer_multiprocess, args=[indexer, document_parsed_list.copy()])
                document_parsed_list = []
                thread_is_on = True
                thread.start()
    if thread is not None and thread.is_alive():
        thread.join()
    if len(document_parsed_list) > 0:
        indexer_multiprocess(indexer, document_parsed_list)
    indexer.empty_pendling()
    indexer.drain_docs()
    indexer.wait_until_stop_write_documents()
    indexer.start_creating_posting_files()
    indexer.drain_all_garage()
    utils.save_obj(indexer.get_inv_idx(), "inverted_idx")


def search_and_rank_query(query, inverted_index, k):
    """
    this function will parse the query, search in the posting files the relevant documents,
    ranked the documents and return the k best ranked documents rationally to the query.
    :param query - information that the user looking for. given in String.
    :param inverted_index - inverted index that contain import details about every word. given in Dictionary.
    :param k - the number of documents the user want to get. given in Int.
    :return a sorted List of the k most relevant documents. return a List.
    """
    p = Parse(inverted_index)
    query_as_list = p.parse_sentence(query)
    query_as_dic = {}
    for token in query_as_list:
        if token in query_as_dic:
            query_as_dic[token] += 1
        else:
            query_as_dic[token] = 1
    searcher = Searcher(inverted_index)
    query_as_dic = searcher.relevant_terms_in_inverted_index(query_as_dic)
    relevant_docs = searcher.relevant_docs_from_posting(query_as_dic)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs, query_as_dic, k)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)

def main(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
    run_engine(output_path, corpus_path, stemming)
    inverted_index = utils.load_invertedIndex()
    wtf = utils.load_inverted_index()
    if not isinstance(queries, list):
        f = open(queries, 'r')
        queries_list = f.read().splitlines()
    else:
        queries_list = queries
    export_to_csv=[]
    for query_idx in range(len(queries_list)):
        query_answer = search_and_rank_query(queries_list[query_idx], inverted_index, num_docs_to_retrieve)
        export_to_csv.append((query_idx,query_answer))
        for doc_tuple in query_answer:
            print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
    save_query_answer_as_csv(export_to_csv)


def save_query_answer_as_csv(answer_list):
    dataFrame = pn.DataFrame(columns=['Query_num', 'Tweet_id', 'Rank'])
    for query_ans in answer_list:
        query_idx=query_ans[0]+1
        doc_list_and_rank=query_ans[1]
        for twit in doc_list_and_rank:
            dataFrame=dataFrame.append({'Query_num': query_idx, 'Tweet_id': twit[0], 'Rank': twit[1]}, ignore_index=True)
    dataFrame.to_csv('results.csv', index=False)
