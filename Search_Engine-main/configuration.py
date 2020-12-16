class ConfigClass:

    _fileCounter = 0
    _limit_file_documents = 5000
    __toStem = False
    def __init__(self):
        self.corpusPath = 'Post'
        self.savedFileMainFolder = ''
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"

        print('Project was created successfully..')

    @staticmethod
    def get_post_count():
        return ConfigClass._fileCounter

    @staticmethod
    def post_count_up():
        ConfigClass._fileCounter += 1

    @staticmethod
    def get_limit():
        return ConfigClass._limit_file_documents

    @staticmethod
    def get_toStem():
        return ConfigClass.__toStem

    @staticmethod
    def set_stem(state):
        if type(state) is bool:
            ConfigClass.__toStem = state;

    def get__corpusPath(self):
        return self.corpusPath
