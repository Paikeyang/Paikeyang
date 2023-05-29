import csv
import os
import random
import json

class HDFS:
    def __init__(self):
        self.data_block_num = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
        if os.path.exists('namenode.json'):
            f = open('namenode.json', 'r')
            self.namenode_dict = json.loads(f.read())
        else:
            self.namenode_dict = {}

    def NameNode(self, operater, name_num=1, block_num=0, row=[], file_name='', JSON=False):
        if operater == 'check':
            checked_list = []
            checking_list = list(range(1, 26))
            while True:
                check_list = list(set(checking_list) - set(checked_list))
                if len(check_list) == 0:
                    raise Exception('No enough nodes')
                random.shuffle(check_list)
                i = check_list[0]
                not_aviable_data_block = []
                for _, j, _ in os.walk('./server/NameNode' + str(i)):
                    not_aviable_data_block = j
                    break
                aviable_data_block = list(set(self.data_block_num) - set(not_aviable_data_block))
                if len(aviable_data_block) == 0:
                    continue
                else:
                    name_num = i
                    random.shuffle(aviable_data_block)
                    block_num = int(aviable_data_block[0])
                    break
            return name_num, block_num

        elif operater == 'write':
            if file_name in self.namenode_dict.keys():
                block_original = self.namenode_dict[file_name][-1]
                name_num, block_num = block_original // 10, block_original % 10
                if JSON == True:
                    if os.path.getsize('./server/NameNode' + str(name_num) +
                                       '/' + str(block_num) + '/' + file_name + '.json') >= 800:
                        name_num, block_num = self.NameNode('check')
                else:
                    if os.path.getsize('./server/NameNode' + str(name_num) +
                                       '/' + str(block_num) + '/' + file_name + '.csv') >= 800:
                        name_num, block_num = self.NameNode('check')
            else:
                name_num, block_num = self.NameNode('check')

            if os.path.exists('./server/NameNode' + str(name_num)) == False:
                os.makedirs('./server/NameNode' + str(name_num))
            if os.path.exists('./server/NameNode' + str(name_num) + '/' + str(block_num)) == False:
                os.makedirs('./server/NameNode' + str(name_num) + '/' + str(block_num))

            if JSON == True:
                if os.path.exists('./server/NameNode' + str(name_num) + '/' + str(block_num) + '/' + file_name + '.json'):
                    with open('./server/NameNode' + str(name_num) + '/' + str(block_num) +
                              '/' + file_name + '.json', 'a') as json_file:
                        json_file.write(row)
                        json_file.write('\n')
                else:
                    with open('./server/NameNode' + str(name_num) + '/' + str(block_num) +
                              '/' + file_name + '.json', 'w') as json_file:
                        json_file.write(row)
                        json_file.write('\n')
            else:
                if os.path.exists('./server/NameNode' + str(name_num) + '/' + str(block_num) + '/' + file_name + '.csv'):
                    with open('./server/NameNode' + str(name_num) + '/' + str(block_num) +
                              '/' + file_name + '.csv', 'a', encoding='utf-8',newline='') as csv_file:
                        writer = csv.writer(csv_file)
                        writer.writerow(row)
                else:
                    with open('./server/NameNode' + str(name_num) + '/' + str(block_num) +
                              '/' + file_name + '.csv', 'w', encoding='utf-8', newline='') as csv_file:
                        writer = csv.writer(csv_file)
                        writer.writerow(row)

            if file_name not in self.namenode_dict.keys():
                self.namenode_dict[file_name] = [int(str(name_num) + str(block_num))]
            elif int(str(name_num) + str(block_num)) not in self.namenode_dict[file_name]:
                self.namenode_dict[file_name].append(int(str(name_num) + str(block_num)))

            self.update_namenode()

        elif operater == 'read':
            block_original = self.namenode_dict[file_name]
            if JSON == True:
                dict_total = []
                for j in block_original:
                    name_num, block_num = j // 10, j % 10
                    with open('./server/NameNode' + str(name_num) + '/' + str(block_num) +
                              '/' + file_name + '.json', 'r') as json_file:
                        for line in json_file:
                            list_temp = list((k, v) for k, v in json.loads(line).items())
                            dict_total.extend(list_temp)
                return dict_total
            else:
                list_total = []
                for i in block_original:
                    name_num, block_num = i // 10, i % 10
                    with open('./server/NameNode' + str(name_num) + '/' + str(block_num) + '/' + file_name + '.csv',
                              'r', encoding='utf-8') as csv_file:
                        reader = csv.reader(csv_file)
                        for row in reader:
                            list_total.append(row)
                return list_total

    def upload_file(self, path, file_name):
        with open(path, 'r', encoding='utf-8') as csv_file:
            reader = csv.reader(csv_file)
            file_name = file_name
            for row in reader:
                if '' in row:
                    continue
                self.NameNode('write', row=row, file_name=file_name)

    def checking(self):
        for i in range(1, 26):
            if os.path.getsize('./server/NameNode' + str(i)) == 0:
                os.remove('./server/NameNode' + str(i))
                continue
            for j in range(10):
                if os.path.getsize('./server/NameNode' + str(i) + '/' + str(j)) == 0:
                    os.remove('./server/NameNode' + str(i) + '/' + str(j))
                    continue

    def update_namenode(self):
        namenode_json = json.dumps(self.namenode_dict)
        f = open('namenode.json', 'w')
        f.write(namenode_json)

hdfs = HDFS()
