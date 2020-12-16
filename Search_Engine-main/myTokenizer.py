import string
from re import findall, sub
from configuration import ConfigClass
import utils
from stemmer import Stemmer


class Tokenizer:

    def __init__(self, inverted_idx,parse):
        self.__word_list = None
        self.__size = None
        self.__UnitName = ["thousand", "million", "billion"]
        self.__Stemmer = Stemmer()
        self.__punctuation = {}
        for item in string.punctuation + '’':
            self.__punctuation[item] = 0
        self.__punctuation[' '] = 0
        self.__inverted_idx = inverted_idx
        self.__constractions = utils.load_obj("constractions")
        self.__parse = parse
        self.__pre_everyword = {ord('’'): ord("'"), ord('"'): None}
        self.__pre_clear_punc = {ord(':'): None, ord(','): None,ord('.'):None,ord(';'):None,ord('!'):None,ord('&'):None,ord('*'):None}
        self.__pre_http = {ord('{'):None,ord('}'):None,ord(','):ord(' '),ord(':'):ord(' ')}

    def preProcess(self,text):
        """
        This class provides tokens for each text in a simple way.
        :param text: text that u want to tokenize.
        :return: list of tokens
        """
        tokens = []
        for word in text.split():
            word = word.translate(self.__pre_everyword)

            if word == '':
                continue

            if word[0] == '@':
                if(word[len(word)-1] == '…'):
                    continue
                word = word.translate(self.__pre_clear_punc)
                word = word[1:len(word)]
                tokens.append('@')
                tokens.append(word.lower())
                continue

            if word[0] == '#':
                if (word[len(word) - 1] == '…'):
                    continue
                word = word.translate(self.__pre_clear_punc)
                word = word[1:len(word)]
                tokens.append('#')
                tokens.append(word)
                continue

            for i in range(0,len(word)):
                if word[i] in self.__punctuation:
                    tokens.append(word[i])
                else:
                    word = word[i:len(word)]
                    break

            append_in_the_end = []
            steps = 0
            for i in range(len(word)-1,0,-1):
                if word[i] in self.__punctuation:
                    append_in_the_end.append(word[i])
                    steps += 1
                else:
                    word = word[0:len(word)-steps]
                    break;

            if word.lower() in self.__constractions:
                word = self.__constractions[word.lower()]

            if 'http' in word and ':' in word:
                if word[0:4] != 'http':
                    link_splited = word.split('http');
                    tokens.extend(self.preProcess(link_splited[0]))
                    word = ''
                    for i in range(1, len(link_splited)):
                        word += 'http' + link_splited[i]

                word = word.translate(self.__pre_http)
                for link in word.split():
                    http_splited = link.split(':')
                    tokens.append(http_splited[0])
                    tokens.append(':')
                continue

            words = []
            current_word = ''
            for i in range(0,len(word)):
                chr = word[i]
                if chr ==  '…':
                    current_word = ''
                    continue
                if chr in self.__punctuation:
                    if chr == '-':
                        if (current_word != ''):
                            if(int(i+1) < len(word) and word[i+1] not in self.__punctuation):
                                current_word += chr
                                continue

                            else:
                                words.append(current_word)
                                current_word = ''
                                words.append(chr)
                                continue
                        else:
                            words.append(chr)
                            continue

                    else:
                        if current_word != '':
                            words.append(current_word)
                            current_word = ''
                        if chr == ' ': continue
                        words.append(chr)
                else:
                    current_word += chr
            if current_word != '':
                words.append(current_word)
            tokens.extend(words)
            tokens.extend(append_in_the_end)

        while '' in tokens:
            tokens.remove('')
        return tokens

    def insert_text(self, text):
        self.__word_list = text
        self.__size = len(text)

    def __remove_punctuation(self,value):
        result = ""
        for c in value:
            # If char is not punctuation, add it to the result.
            if c not in string.punctuation:
                result += c
        return result

    def __isDigit(self,token):
        point_counter = 0
        comma_counter = 0
        for i in token:
            if str(i) == ",":
                comma_counter += 1
                if(comma_counter >= 2): return False
                continue
            if str(i) == ".":
                point_counter += 1
                if(point_counter >= 2): return False
                continue
            if ord(i) > 57 or ord(i) < 48: return False
        return True

    def __handle_tag(self,token):
        res = []
        res.append('@' + token)
        return res

    def __handle_precent(self,token):
        res = []
        res.append(token + "%")
        return res

    def __handle_rest_case(self,token, i):
        res = []
        if (int(i + 1) < int(self.__size) and
                token[len(token) - 1] == self.__word_list[i + 1][0]
                and len(token) == 3 and len(self.__word_list[i + 1]) == 2):
            token += self.__word_list[i + 1]
            res.append(token)
            i += 2
            return (res, i)

        res.extend(self.__handle_word(self.__word_list[i]))
        return (res, i + 1)

    def __isUpper(self,chr):
        if ord(chr) >= 65 and ord(chr) <= 90:
            return True
        return False

    def __handle_entities(self,token, i):
        res = []
        entitie = token.upper()
        index = int(i)
        while (int(index) < int(self.__size) and
               self.__isUpper(self.__word_list[index][0])):

            token = self.__word_list[index]

            if ConfigClass.get_toStem():
                token = self.__Stemmer.stem_term(token)

            if self.__inverted_idx.get(token.lower(), 0) == 0:
                res.append(token.upper())
            else:
                res.append(token.lower())

            if ('-' in self.__word_list[index]):
                split_words = self.__word_list[index].split('-')
                for word in split_words:
                    res.append(word.upper())

            if (index > i):
                entitie += " " + self.__word_list[index].upper()
                res.append(entitie)

            index += 1

        return (res, index)

    def __handle_numbers(self,token):

        res = []
        with_unit = False
        with_dot = False
        with_dollar = False
        size_after_dot = 0
        size_before_dot = 0
        num_before_dot = ""
        num_after_dot = ""
        token = token.replace(',','')
        if '$' in token:
            with_dollar = True
            token = token.replace('$','')

        if '/' in token:
            res.append(token)
            return res

        if ' ' in token: with_unit = True
        if '.' in token: with_dot = True

        if (with_unit):
            splited_token = token.split(' ')
            num_before_dot = splited_token[0]
            unit = splited_token[1]
            if with_dot:
                splited_num = num_before_dot.split('.')
                num_before_dot = splited_num[0]
                num_after_dot = splited_num[1]
                size_after_dot = len(num_after_dot)
                if size_after_dot > 3:
                    num_after_dot = num_after_dot[0:3]

            add = num_before_dot
            if with_dot:
                add += '.' + num_after_dot

            if unit == "thousand":
                res.append(add + 'K')

            elif unit == "million":
                res.append(add + 'M')

            elif unit == "billion":
                res.append(add + 'B')

        else:
            if with_dot:
                splited_num = token.split('.')
                num_before_dot = splited_num[0]
                num_after_dot = splited_num[1]
                size_before_dot = len(num_before_dot)
                size_after_dot = len(num_after_dot)
            else:
                num_before_dot = token
                size_before_dot = len(num_before_dot)

            if int(size_before_dot) < 4:
                if with_dot:
                    if int(size_after_dot) < 3:
                        res.append(num_before_dot + '.' + num_after_dot)
                    else:
                        res.append(num_before_dot + '.' + num_after_dot[0:3])
                else:
                    res.append(num_before_dot)

            elif int(size_before_dot) < 7:
                num = float(num_before_dot)
                num = num / 1000
                num_before_dot = str(num)
                res.append(num_before_dot + 'K')

            elif int(size_before_dot) < 10:
                num = float(num_before_dot)
                num = int(num / 1000)
                num = float(num / 1000)
                num_before_dot = str(num)
                res.append(num_before_dot + 'M')

            else:
                num = float(num_before_dot)
                num = int(num / 1000000)
                num = float(num / 1000)
                num_before_dot = str(num)
                res.append(num_before_dot + 'B')

        if(with_dollar):
            res[0] = res[0]+"$"
        return res

    def __handle_url(self,token):
        res = []
        splited = token.split(" ")
        res.append(splited[0])
        t = splited[1]
        t = sub(r'[^\w\s.]', '/', t)
        t = t.split('/')
        for part in t:
            if 'www' in part:
                res.append('www')
                res.append(part[4:].lower())
                continue
            if part == '':
                continue
            res.append(part.lower())
        return res

    def __handle_word(self,token):
        res = []
        if '-' in token:
            splited_token = token.split('-')
            for word in splited_token:
                res.append(word.upper())
            res.append(token)
            return res
        token = token.lower()
        res.append(token)
        return res

    def __handle_hashtag(self,token):
        unfiltered_res = []
        res = []
        token = token[0].upper() + token[1:]
        words = findall('[A-Z][^A-Z]*', token)
        for word in words:
            word = word.upper()
            to_append = word.split('_')
            unfiltered_res.extend(to_append)
        while '' in unfiltered_res:
            unfiltered_res.remove('')
        res.append('#' + str.join('', unfiltered_res).lower())
        unfiltered_res_length = len(unfiltered_res)
        i = 0
        while int(i) < int(unfiltered_res_length):
            word = ''
            if len(unfiltered_res[i]) == 1:
                word += unfiltered_res[i]

                while int(i+1) < int(unfiltered_res_length) and (len(unfiltered_res[i+1])==1 or unfiltered_res[i+1][-1].isdigit()):
                    word += unfiltered_res[i+1]
                    i += 1

            else:
                word = unfiltered_res[i]

            if word.lower() not in self.__parse.stop_words:
                res.append(word)

            i += 1

        return res

    def tokenize(self):
        """
            This method provides tokens by roles that fit to twitter messages.
             from the tokens that came from the method insert_text.
            :return: list of tokens
        """
        result = []
        i = 0
        tokens_number = len(self.__word_list)

        while int(i) < int(tokens_number):
            if (self.__word_list[i] == ''):
                i += 1
                continue

            if '…' == self.__word_list[i][len(self.__word_list[i])-1]:
                i += 1
                continue

            if ord(self.__word_list[i][0]) > 128:
                i += 1
                continue

            if ord(self.__word_list[i][len(self.__word_list[i])-1]) > 128:
                self.__word_list[i] = ''.join(k for k in self.__word_list[i] if ord(k)<128)

            if self.__word_list[i] == '':
                i += 1
                continue

            if self.__word_list[i] == '@':
                if i + 1 == tokens_number:
                    i += 1
                    continue
                result.extend(self.__handle_tag(self.__word_list[i + 1]))
                i += 2
                continue

            if self.__word_list[i] == '#':
                if i + 1 == tokens_number:
                    i += 1
                    continue
                result.extend(self.__handle_hashtag(self.__word_list[i + 1]))

                i += 2
                continue

            if len(self.__word_list[i]) == 1:
                if self.__word_list[i] in self.__punctuation:
                    i += 1
                    continue

            if 'http' in self.__word_list[i] and int(i + 2) < int(self.__size):
                result.extend(self.__handle_url(self.__word_list[i] + " " + self.__word_list[i + 2]))
                i += 3
                continue

            if self.__isDigit(self.__word_list[i]):
                if ((int(i) < int(self.__size - 1)) and (self.__word_list[i + 1] == '%' or "percent" in self.__Stemmer.stem_term(
                        self.__word_list[i + 1]))):
                    result.extend(self.__handle_precent(self.__word_list[i]))
                    i += 2
                    continue

                if int(i) < int(self.__size - 1) and '$' in self.__word_list[i + 1]:
                    if int(i) < int(self.__size - 2) and self.__Stemmer.stem_term(self.__word_list[i + 2]) in self.__UnitName:
                        result.extend(self.__handle_numbers(self.__word_list[i]+" "+self.__Stemmer.stem_term(self.__word_list[i + 2])+ '$'))
                        i += 3
                        continue
                    result.extend(self.__handle_numbers(self.__word_list[i] + '$'))
                    i += 2
                    continue

                else:
                    if (int(i) < int(self.__size - 2) and self.__word_list[i + 1] == '.'
                            and self.__word_list[i + 2].isdigit()):
                        if int(i) < int(self.__size - 3) and self.__Stemmer.stem_term(self.__word_list[i + 3]) in self.__UnitName:
                            result.extend(self.__handle_numbers(self.__word_list[i] + '.' + self.__word_list[i + 2]
                                                         + ' ' + self.__Stemmer.stem_term(self.__word_list[i + 3])))
                            i += 4
                            continue
                        else:
                            result.extend(self.__handle_numbers(self.__word_list[i] + '.' + self.__word_list[i + 2]))
                            i += 3
                            continue

                    elif int(i) < int(self.__size - 1) and self.__Stemmer.stem_term(self.__word_list[i + 1]) in self.__UnitName:
                        result.extend(self.__handle_numbers(
                            self.__word_list[i] + ' ' + self.__Stemmer.stem_term(self.__word_list[i + 1])))
                        i += 2
                        continue

                    elif (int(i) < int(self.__size - 3) and self.__word_list[i + 1].isdigit()
                          and self.__word_list[i + 2] == '/' and self.__word_list[i + 3].isdigit()):
                        result.extend(self.__handle_numbers(self.__word_list[i]
                                                     + ' ' + self.__word_list[i + 1] + self.__word_list[i + 2] +
                                                     self.__word_list[i + 3]))
                        i += 4
                        continue

                    else:
                        result.extend(self.__handle_numbers(self.__word_list[i]))
                        i += 1
                        continue

            if (self.__word_list[i][0] in self.__punctuation
                    or self.__word_list[i][len(self.__word_list[i]) - 1] in self.__punctuation):

                self.__word_list[i] = self.__remove_punctuation(self.__word_list[i])
                if (self.__word_list[i].lower() in self.__parse.stop_words or self.__word_list[i] == ''):
                    i += 1
                    continue

            if self.__word_list[i][0].isupper():
                entities_res = self.__handle_entities(self.__word_list[i], i)
                result.extend(entities_res[0])
                i = entities_res[1]
                continue

            else:
                rest_case_result = self.__handle_rest_case(self.__word_list[i], i)
                result.extend(rest_case_result[0])
                i = rest_case_result[1]
                continue

        return result
