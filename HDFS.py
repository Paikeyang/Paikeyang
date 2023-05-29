import csv
import os
import random
import json


class HDFS:
    def __init__(self):
        self.data_block_num = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
        if os.path.exists('namenode.json'):
            f = open('namenode.json', 'r')         # initial total data block number in one namenode
            self.namenode_dict = json.loads(f.read())         # loading namenode records if exist
        else:
            self.namenode_dict = {}        # The first time run will create a blank dictionary to recording.

    def NameNode(self, operater, name_num=1, block_num=0, row=[], file_name='', JSON=False):
        if operater == 'check':
            return self.check_node()                     # NameNode can check available datablock location,
        elif operater == 'write':
            self.write_block(file_name, row, JSON=JSON)       # also can write and read files seperated
        elif operater == 'read':                              # on different datablocks
            return self.read_file(file_name, JSON=JSON)

    def write_block(self, file_name, row, JSON=False):                                  # check if it needs
        name_num, block_num, exist_file = self.judge_new_block(file_name, JSON=JSON)  # a new datablock
        self.make_dirs(name_num, block_num)           # create directory if no exist
        if JSON:                        # There are two file types, .json and .csv
            self.json_datablock_write(file_name, name_num, block_num, exist_file, row)
        else:
            self.csv_datablock_write(file_name, name_num, block_num, exist_file, row)
        self.namenode_dict = self.update_namenode_dict(file_name, self.namenode_dict, name_num, block_num)
        self.update_namenode()                     # update the namenode_dict and save the namenode_dict

    def judge_new_block(self, file_name, JSON=False):
        if JSON:
            suffix = '.json'
        else:
            suffix = '.csv'
        if file_name in self.namenode_dict.keys():        # check if file exist in namenode record
            block_original = self.namenode_dict[file_name][-1]
            name_num, block_num = block_original // 10, block_original % 10
            exist_file = 1

            if os.path.getsize('./server/NameNode' + str(name_num) +
                               '/' + str(block_num) + '/' + file_name + suffix) >= 800:
                name_num, block_num = self.NameNode('check')     # each datablock is in size 1000 bytes
                exist_file = 0                              # if file sizes already exceed 80 % of the total size
        else:                                                      # then find a new datablock
            name_num, block_num = self.NameNode('check')
            exist_file = 0                          # if file not exist, also find a new datablock
        return name_num, block_num, exist_file

    def csv_datablock_write(self, file_name, name_num, block_num, exist_file, row):
        if exist_file == 1:
            with open('./server/NameNode' + str(name_num) + '/' + str(block_num) +
                      '/' + file_name + '.csv', 'a', encoding='utf-8', newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(row)
        elif exist_file == 0:                      # csv writer will determine if write in add way or cover way
            with open('./server/NameNode' + str(name_num) + '/' + str(block_num) +
                      '/' + file_name + '.csv', 'w', encoding='utf-8', newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(row)

    def json_datablock_write(self, file_name, name_num, block_num, exist_file, row):
        if exist_file == 1:
            self.json_write('./server/NameNode' + str(name_num) + '/' + str(block_num) +
                            '/' + file_name + '.json', row, model='a')
        elif exist_file == 0:                         # json writer will determine if write in add way or cover way
            self.json_write('./server/NameNode' + str(name_num) + '/' + str(block_num) +
                            '/' + file_name + '.json', row)

    def check_node(self):
        checked_list = []
        checking_list = list(range(1, 51))          # There are totally 50 namenodes
        while True:
            check_list = list(set(checking_list) - set(checked_list))
            if len(check_list) == 0:
                raise Exception('No enough nodes')
            random.shuffle(check_list)                 # randomly select a namenode
            i = check_list[0]
            not_available_data_block = []
            for _, j, _ in os.walk('./server/NameNode' + str(i)):
                not_available_data_block = j
                break
            available_data_block = list(set(self.data_block_num) - set(not_available_data_block))
            if len(available_data_block) == 0:
                checked_list.append(i)             # available if there are no datablock number exist
                continue                                 # append to the checked list, next loop wouldn't check
            else:                                     # this namenode again
                name_num = i
                random.shuffle(available_data_block)        # if remain available datablock, choose one randomly
                block_num = int(available_data_block[0])
                break
        return name_num, block_num

    def read_file(self, file_name, JSON=False):
        block_original = self.namenode_dict[file_name]     # files will read by row adding into list_total
        list_total = []
        for i in block_original:
            name_num, block_num = i // 10, i % 10         # get the location of the datablock
            if JSON:
                with open('./server/NameNode' + str(name_num) + '/' + str(block_num) +
                          '/' + file_name + '.json', 'r') as json_file:
                    for line in json_file:
                        list_temp = list((k, v) for k, v in json.loads(line).items())
                        list_total.extend(list_temp)
            else:
                with open('./server/NameNode' + str(name_num) + '/' + str(block_num) + '/' + file_name + '.csv',
                          'r', encoding='utf-8') as csv_file:
                    reader = csv.reader(csv_file)
                    for row in reader:
                        list_total.append(row)
        return list_total

    def upload_file(self, path, file_name):
        with open(path, 'r', encoding='utf-8') as csv_file:
            reader = csv.reader(csv_file)                       # upload files by row
            file_name = file_name
            for row in reader:
                if '' in row:
                    continue
                self.NameNode('write', row=row, file_name=file_name)

    def update_namenode_dict(self, file_name, namenode_dict, name_num, block_num):
        if file_name not in namenode_dict.keys():
            namenode_dict[file_name] = [int(str(name_num) + str(block_num))]          # create a key if no target key
        elif int(str(name_num) + str(block_num)) not in namenode_dict[file_name]:   # append new value in the dictionary
            namenode_dict[file_name].append(int(str(name_num) + str(block_num)))   # if key already exist
        return namenode_dict

    def checking(self):
        for i in range(1, 51):
            if os.path.getsize('./server/NameNode' + str(i)) == 0:   # This function is using for checking if dictionary
                os.remove('./server/NameNode' + str(i))          # is blank, if yes, delete it in location
                continue
            for j in range(10):
                if os.path.getsize('./server/NameNode' + str(i) + '/' + str(j)) == 0:
                    os.remove('./server/NameNode' + str(i) + '/' + str(j))
                    continue

    def update_namenode(self):
        namenode_json = json.dumps(self.namenode_dict)
        f = open('namenode.json', 'w')           # save the namenode in namenode.json
        f.write(namenode_json)

    def make_dirs(self, name_num, block_num):
        if os.path.exists('./server/NameNode' + str(name_num)) == False:
            os.makedirs('./server/NameNode' + str(name_num))                     # create directory if it doesn't exist
        if os.path.exists('./server/NameNode' + str(name_num) + '/' + str(block_num)) == False:
            os.makedirs('./server/NameNode' + str(name_num) + '/' + str(block_num))

    def json_write(self, file_name, file, model='w'):
        dict_file = json.dumps(file)                       # write json files
        f = open(file_name, model)
        f.write(dict_file)
        f.write('\n')


hdfs = HDFS()
