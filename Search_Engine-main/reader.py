import os
import pandas as pd
import queue


class ReadFile:

    def __initQueue(self):
        for root, dirs, files in os.walk(self.corpus_path):
            for file in files:
                try:
                    if file.endswith(".parquet"):
                        rel_dir = os.path.relpath(root,self.corpus_path)
                        rel_file = os.path.join(rel_dir,file)
                        self.queue.put(rel_file)
                except:
                    continue

    def __init__(self, corpus_path):
        self.corpus_path = corpus_path
        self.queue = queue.Queue()
        self.__initQueue()

    def read_file(self, file_name):
        """
        This function is reading a parquet file contains several tweets
        The file location is given as a string as an input to this function.
        :param file_name: string - indicates the path to the file we wish to read.
        :return: a dataframe contains tweets.
        """
        # full_path = os.path.join(self.corpus_path +'/' + file_name)
        df = pd.read_parquet(self.corpus_path+'/'+file_name, engine="pyarrow")
        return df.values.tolist()

    def __iter__(self):
        return self

    def __next__(self):
        if self.queue.empty():
            raise StopIteration
        file_path = self.queue.get()
        return self.read_file(file_path)

    def getNext(self):
        if self.queue.empty():
            return None
        else:
            file_path = self.queue.get()
            return self.read_file(file_path)
