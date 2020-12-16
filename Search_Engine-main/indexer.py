import pickle
import threading
from math import log10
import math
import os
from concurrent.futures import ThreadPoolExecutor


class Indexer:
    """
    Creating inverted index as Dictionary - key= term , value = tuple(number of appear in entire corpus,df,line number in posting file)
    an addition to creating posting files - ordered by first char of the term (in case that the term isn't word it will save in 'simbols.pkl' file
    """

    def __init__(self, config, garage_limit, output_path, ram_drain_limit=15 * 10 ** 6):
        self.__ram_drain_limit = ram_drain_limit  # garage dict writing limit
        self.__inverted_idx = {
            '__output_path__': output_path,
            '__doc_number__': 0}  # key -> tuple(number of appear in entire corpus,df,line number in posting file,file_ID)
        self.__config = config
        self.__garage_dict = {0: {}}  # will save word untile arriving to limit then its draing to posting file
        self.__output_path = output_path
        self.__garage_limit = garage_limit  # its state limit to specific garage - when a least 5 garages arrived to this limit they will send to daring
        self.__pending_list = {}
        self.__next_file_ID = 0  # its help to set for each word which file is belong to
        self.__next_file_ID_counter = 0
        self.__garage_values_counter = {0: 0}  # count how much values in each garage holding in ram
        self.__inverted_doc = {}
        self.__doc_counter = 0
        self.__drain_docs_thread = []
        self.__data_in_ram = 0  # count how much writing to garages holding in ram
        self.__next_index_in_file = {0: 0}
        self.__id_file_part = {0: 0}

    def __delete_doc_after_use(self, doc_number):
        """

        :param doc_number: doc to remove while indexer still working (using in thread)
        :return: None
        """
        try:
            os.remove(self.__output_path + '/' + 'd' + str(doc_number) + ".pkl")
        except:
            print(self.__output_path + '/' + 'd' + str(doc_number) + ".pkl : does not exist, delete fail.")

    def __delete_file_after_use(self, file_name):
        """

        :param doc_number: doc to remove while indexer still working (using in thread)
        :return: None
        """
        try:
            os.remove(self.__output_path + '/' + file_name + ".pkl")
        except:
            print(self.__output_path + '/' + 'd' + file_name + ".pkl : does not exist, delete fail.")

    def wait_until_stop_write_documents(self):
        for i in self.__drain_docs_thread:
            if i.is_alive():
                i.join()

    def changeTerm(self, old, new):
        """

        :param old: older term
        :param new: target to change to
        :return:
        """
        if old not in self.__inverted_idx or new == old:
            return
        temp = self.__inverted_idx.pop(old)
        self.__inverted_idx[new] = temp

    def get_inv_idx(self):
        return self.__inverted_idx

    def add_new_doc(self, document):

        terms_dic = document.term_doc_dictionary
        key_list = terms_dic.keys()
        self.__inverted_idx['__doc_number__'] += 1
        for key in key_list:
            if len(key) == 0: continue
            term_dic_value = terms_dic[key]
            if key.lower() in self.__inverted_idx and key.isupper():
                key = key.lower()
            if key.upper() in self.__inverted_idx and key.islower():
                self.changeTerm(key.upper(), key.lower())
            if len(key) == 0:
                continue
            if len(key[0]) > 1:
                continue
            garage_number = self.__get_file_ID(key)
            if key in self.__inverted_idx:
                updating_inverted_index = (
                    self.__inverted_idx[key][0] + term_dic_value, self.__inverted_idx[key][1] + 1,
                    self.__inverted_idx[key][2], self.__inverted_idx[key][3])
            else:
                updating_inverted_index = (term_dic_value, 1, None, garage_number)

            self.__inverted_idx[key] = updating_inverted_index

        entites_dict = document.entites
        for entity in entites_dict.keys():
            if len(entity) == 0: continue
            if entity in self.__inverted_idx:
                garage_number = self.__get_file_ID(entity)
                self.__inverted_idx[entity] = (
                    self.__inverted_idx[entity][0] + entites_dict[entity], self.__inverted_idx[entity][1] + 1,
                    None, garage_number)
            else:
                if entity in self.__pending_list:
                    garage_number = self.__get_file_ID(entity)
                    self.__inverted_idx[entity] = (
                        entites_dict[entity] + self.__pending_list[entity], 2, None, garage_number)
                else:
                    self.__pending_list[entity] = entites_dict.get(entity)

        self.__inverted_doc[int(document.get_twit_id())] = (
            document.get_max_tf(), document.get_unique_words_amount(), document.term_doc_dictionary, document.entites)

        if len(self.__inverted_doc) > 20000:
            # if self.__drain_docs_thread is not None and self.__drain_docs_thread.is_alive():
            #     self.__drain_docs_thread.join()
            self.__drain_docs_thread.append(
                threading.Thread(target=self.drain_docs_thread, args=[self.__inverted_doc.copy()]))
            self.__drain_docs_thread[-1].start()
            self.__inverted_doc = {}
            self.__doc_counter += 1

    def drain_docs(self):
        """
        After creating inverted index - we want to drain all documents that still doesnt save in hard disk
        :return: None
        """
        if len(self.__inverted_doc) > 0:
            file = open(self.__output_path + '/' + 'd' + str(self.__doc_counter) + ".pkl", "wb")
            pickle.dump(self.__inverted_doc, file, pickle.HIGHEST_PROTOCOL)
            file.close()
            self.__inverted_doc = {}
            self.__doc_counter += 1

    def drain_docs_thread(self, inverted_doc_copy):
        """

        :param inverted_doc_copy: list of document we want to save in hard disk
        :return:
        """
        file = open(self.__output_path + '/' + 'd' + str(self.__doc_counter) + ".pkl", "wb")
        pickle.dump(inverted_doc_copy, file, pickle.HIGHEST_PROTOCOL)
        file.close()

    def start_creating_posting_files(self):
        """
        This function running on all documents fill we create in the first stage and send it
        \nto function that building that posting files\n
        while building the posting files the documents files will erase
        :return: None
        """
        for i in range(self.__doc_counter - 1, -1, -1):
            file = open(self.__output_path + '/' + 'd' + str(i) + ".pkl", "rb")
            documents = pickle.load(file)
            file.close()
            for twit_id in documents:
                term_doc = documents[twit_id][2]
                entite_doc = documents[twit_id][3]
                max_tf = documents[twit_id][0]
                unique_terms = documents[twit_id][1]
                self.building_posting_file(twit_id, term_doc, entite_doc, max_tf, unique_terms)
            threading.Thread(target=self.__delete_doc_after_use, args=[i]).start()

    def building_posting_file(self, twit_id, term_doc, entite_doc, max_tf, unique_words):
        """
        this function iterate document object and extract data to posting files - separated by the first latter of
        the term an addition it iterate on entity that collected in parse stage and check if its suppose to join to
        the inverted index (if entity exist more then once its join to inverted index) /n This function fill a
        dictionary called grage - each letter have a representation in this dictionary - when some dictionary contain
        more then limit (defined in the constructor) its call to function that drain all the latter data to that file
        its belong to - when we cross this limit the function send this dictionary and the 4 biggest dictionary to drain
        :param document: document

        object :return: None
        """
        terms_dic = term_doc
        key_list = terms_dic.keys()
        # calculate data for using Cosim similarity
        square_weight_tf_idf = 0
        weight_doc_by_tf_idf = {}
        for key in key_list:
            if len(key) == 0: continue
            if key not in self.__inverted_idx:
                if key.upper() in self.__inverted_idx:
                    token = key.upper()
                elif key.lower() in self.__inverted_idx:
                    token = key.lower()
            else:
                token = key
            tf_ij = terms_dic[key] / unique_words
            weight_doc_by_tf_idf[key] = tf_ij * math.log10(
                self.__inverted_idx['__doc_number__'] / self.__inverted_idx[token][1])
            square_weight_tf_idf += math.pow(weight_doc_by_tf_idf[key], 2)

        for key in entite_doc:
            if len(key) == 0: continue
            if key not in self.__inverted_idx:
                continue
            tf_ij = entite_doc[key] / unique_words
            weight_doc_by_tf_idf[key] = tf_ij * math.log10(
                self.__inverted_idx['__doc_number__'] / self.__inverted_idx[key][1])
            square_weight_tf_idf += math.pow(weight_doc_by_tf_idf[key], 2)

        # starting insert data to posting garage

        for key in key_list:
            if len(key) == 0: continue
            token = key
            if key not in self.__inverted_idx:
                if key.upper() in self.__inverted_idx:
                    token = key.upper()
                elif key.lower() in self.__inverted_idx:
                    token = key.lower()
            else:
                token = key

            garage_name = self.__get_file_ID(token)
            relevant_dict = self.__garage_dict[garage_name]
            if token in relevant_dict:
                relevant_dict[token].append(
                    (twit_id, terms_dic[key], max_tf, unique_words, weight_doc_by_tf_idf[key], square_weight_tf_idf))
            else:
                relevant_dict[token] = [
                    (twit_id, terms_dic[key], max_tf, unique_words, weight_doc_by_tf_idf[key], square_weight_tf_idf)]

            self.__inverted_idx[token] = (
                self.__inverted_idx[token][0], self.__inverted_idx[token][1],
                self.__inverted_idx[token][2], garage_name)

            self.__garage_values_counter[garage_name] += 1
            self.__data_in_ram += 1

        ###--- handle pending list - entites ---###
        entites_dict = entite_doc
        for entity in entites_dict:
            if len(entity) == 0: continue
            garage_name = self.__get_file_ID(entity)
            if entity not in self.__inverted_idx:
                continue
            else:
                if entity in self.__garage_dict[garage_name]:
                    self.__garage_dict[garage_name][entity].append(
                        (twit_id, entites_dict[entity], max_tf, unique_words, weight_doc_by_tf_idf[entity],
                         square_weight_tf_idf))
                    self.__data_in_ram += 1
                else:
                    self.__garage_dict[garage_name][entity] = [(
                        twit_id, entites_dict[entity], max_tf, unique_words, weight_doc_by_tf_idf[entity],
                        square_weight_tf_idf)]
                    self.__garage_values_counter[garage_name] += 1
                    self.__data_in_ram += 1

        ###--- done handle entity ---###

        # checking if there is a reason to send some data to hard disk
        # simbols fill much faster then other garage so those limit define double then the definition

        if self.__data_in_ram > self.__ram_drain_limit:
            self.drain_all_garage(while_running=True)
            return

        thread_list = []
        sizeList = self.__garage_values_counter.items()
        sizeList = sorted(sizeList, reverse=True, key=lambda x: x[1])
        if sizeList[4][1] >= self.__get_garage_limit():
            thread_list.append(threading.Thread(target=self.__drain_garage, args=[sizeList[0][0]]))
            thread_list.append(threading.Thread(target=self.__drain_garage, args=[sizeList[1][0]]))
            thread_list.append(threading.Thread(target=self.__drain_garage, args=[sizeList[2][0]]))
            thread_list.append(threading.Thread(target=self.__drain_garage, args=[sizeList[3][0]]))
            thread_list.append(threading.Thread(target=self.__drain_garage, args=[sizeList[4][0]]))
            self.__data_in_ram -= sum([i[1] for i in sizeList[:5]])

        for thread in thread_list:
            thread.start()

        for thread in thread_list:
            thread.join()

    def __drain_garage(self, garage_name):
        """
        This function drain all data saved in 'garage_name' dictionary to the matching file
        :param garage_name: garage name we want to drain (represent by - self.__garage_dict[garage_name])
        :return: None
        """

        posting_data = [[] for i in range(self.__next_index_in_file[garage_name])]
        relevant_garage = self.__garage_dict[garage_name]
        for key in relevant_garage.keys():
            if self.__inverted_idx[key][2] is not None:
                posting_data[self.__inverted_idx[key][2]].extend(relevant_garage[key])

            else:
                self.__inverted_idx[key] = (
                    self.__inverted_idx[key][0], self.__inverted_idx[key][1],
                    int(len(posting_data)), garage_name)
                new_value = relevant_garage[key]
                posting_data.append(new_value)
                self.__next_index_in_file[garage_name] += 1

        file = open(self.__output_path + '/' + str(garage_name) + "_" + str(self.__id_file_part[garage_name]) + ".pkl",
                    "wb")
        pickle.dump(posting_data, file)
        file.close()
        self.__garage_dict[garage_name] = {}
        self.__garage_values_counter[garage_name] = 0
        self.__id_file_part[garage_name] += 1

    # def __drain_garage(self, garage_name):
    #     """
    #     This function drain all data saved in 'garage_name' dictionary to the matching file
    #     :param garage_name: garage name we want to drain (represent by - self.__garage_dict[garage_name])
    #     :return: None
    #     """
    #     try:
    #         file = open(self.__output_path + '/' + str(garage_name) + ".pkl", "rb")
    #         posting_data = pickle.load(file)
    #         file.close()
    #     except FileNotFoundError:
    #         posting_data = []
    #     char_garage = self.__garage_dict[garage_name]
    #     for key in char_garage.keys():
    #         if self.__inverted_idx[key][2] is not None:
    #             posting_data[self.__inverted_idx[key][2]].extend(char_garage[key])
    #
    #         else:
    #             self.__inverted_idx[key] = (
    #                 self.__inverted_idx[key][0], self.__inverted_idx[key][1],
    #                 int(len(posting_data)), garage_name)
    #             new_value = char_garage[key]
    #             posting_data.append(new_value)
    #
    #     file = open(self.__output_path + '/' + str(garage_name) + ".pkl", "wb")
    #     pickle.dump(posting_data, file)
    #     file.close()
    #     self.__garage_dict[garage_name] = {}
    #     self.__garage_values_counter[garage_name] = 0

    def drain_all_garage(self, while_running=False):
        """
        Iterate on each key in garage dictionary and send it to drain garage (private method)
        :return:None
        """
        thread_list = []
        # for key in self.__garage_dict.keys():
        #     if self.__garage_values_counter[key] > 0:
        #         if while_running and int(self.__garage_values_counter[key]) < 2000:
        #             continue
        #         self.__data_in_ram -= self.__garage_values_counter[key]
        #         thread_list.append(threading.Thread(target=self.__drain_garage,args=[key]))
        with ThreadPoolExecutor(max_workers=10) as excutor:
            for key in self.__garage_dict.keys():
                if self.__garage_values_counter[key] > 0:
                    if while_running and int(self.__garage_values_counter[key]) < 2000:
                        continue
                    self.__data_in_ram -= self.__garage_values_counter[key]
                    excutor.submit(self.__drain_garage, key)
            excutor.shutdown(True)
        # thread_index = 0
        # while int(thread_index) < len(thread_list):
        #     for i in range(thread_index, min(thread_index + 5, len(thread_list))):
        #         thread_list[i].start()
        #     for i in range(thread_index, min(thread_index + 5, len(thread_list))):
        #         thread_list[i].join()
        #     thread_index += 5
        if not while_running:
            self.__summary_data()

    def __get_file_ID(self, key):
        """
        if term in inverted index its already have a posting file number \n
        otherwise- its will get a number and push it into the data that saved in inverted index
        :param key: the term we want to get his posting file number
        :return: numbert that represent the posting file that 'key' saving in
        """
        if key in self.__inverted_idx:
            return self.__inverted_idx[key][3]
        else:
            if self.__next_file_ID_counter > 2 * self.__next_file_ID + 2:
                self.__next_file_ID += 1
                self.__garage_dict[self.__next_file_ID] = {}
                self.__garage_values_counter[self.__next_file_ID] = 0
                self.__next_file_ID_counter = 0
                self.__next_index_in_file[self.__next_file_ID] = 0
                self.__id_file_part[self.__next_file_ID] = 0
            self.__next_file_ID_counter += 1
            return self.__next_file_ID

    def __summary_data(self):
        data_file = [f for f in os.listdir(self.__output_path) if
                     os.path.isfile(os.path.join(self.__output_path, f)) and f[-3:] == 'pkl']
        files_order = {}
        for file in data_file:
            f_name, f_part = file.split('_')
            if f_name in files_order:
                files_order[f_name] += 1
            else:
                files_order[f_name] = 0
        thread_list = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for file_name in files_order.keys():
                executor.submit(self.__summery_file(file_name, files_order[file_name]))
            # executor.shutdown(True)
        #     for file_name in files_order.keys():
        #         thread_list.append(threading.Thread(target=self.__summery_file, args=[file_name,files_order[file_name]]))
        #
        # thread_index = 0
        # while int(thread_index) < len(thread_list):
        #     for i in range(thread_index, min(thread_index + 5, len(thread_list))):
        #         thread_list[i].start()
        #     for i in range(thread_index, min(thread_index + 5, len(thread_list))):
        #         thread_list[i].join()
        #     thread_index += 5

    def __summery_file(self, file, number_of_parts):
        result = [[] for i in range(self.__next_index_in_file[int(file)])]
        for i in range(number_of_parts + 1):
            file_posting = open(self.__output_path + '/' + str(file) + "_" + str(i) + ".pkl", "rb")
            posting_data = pickle.load(file_posting)
            file_posting.close()
            for row_index in range(len(posting_data)):
                result[row_index].extend(posting_data[row_index])
            threading.Thread(target=self.__delete_file_after_use, args=[file + "_" + str(i)]).start()
        saving_file = open(self.__output_path + '/' + str(file) + ".pkl", "wb")
        pickle.dump(result, saving_file)

    def __get_garage_limit(self):
        """
        its calculate by the posting file amount how when to drain the data holding in ram to hard disk
        :return: the limit as number (float)
        """
        return self.__garage_limit / log10(max(10, self.__next_file_ID))

    def empty_pendling(self):
        """
        empty pendling list
        :return: None
        """
        self.__pending_list = {}
