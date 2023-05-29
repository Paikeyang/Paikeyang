from io import StringIO
import json
import HDFS
import csv

hdfs = HDFS.hdfs

class MapReduce:
    def __init__(self):
        pass

    def Split(self, block_dict, name_list):
        cache = StringIO()
        file_name, block_original = list(block_dict.keys())[0], list(block_dict.values())[0]
        for i in block_original:
            name_num, block_num = i // 10, i % 10
            k1 = i
            with open('./server/NameNode' + str(name_num) + '/' + str(block_num) + '/' + file_name + '.csv', 'r',
                      encoding='utf-8') as csv_file:
                reader = csv.reader(csv_file)
                for row in reader:
                    v1 = row
                    dict1 = {k1: v1}
                    dict2 = self.Map(dict1)
                    cache.write(json.dumps(dict2) + '\n')
                    if len(cache.getvalue()) >= 2000:
                        cache.seek(0)
                        self.Shuffle(cache, file_name, name_list, combine=True, temp='map')
                        cache.close()
                        cache = StringIO()
        cache.seek(0)
        self.Shuffle(cache, file_name, name_list, combine=True, temp='map')
        cache.close()

    def Map(self, dict1):
        dict2 = {}
        for i in list(dict1.values())[0]:
            dict2[i] = [1]
        return dict2

    def Shuffle(self, cache, file_name, name_list, combine=False, temp=''):
        dict3 = {}
        for line in cache.readlines():
            dict2 = json.loads(line)
            for i in name_list:
                if i not in dict3.keys():
                    dict3[i] = {}
            for n, j in enumerate(list(dict2.keys())):
                if j not in dict3[name_list[n]].keys():
                    dict3[name_list[n]][j] = dict2[j]
                elif combine == True:
                    dict3[name_list[n]][j] = [dict3[name_list[n]][j][0] + dict2[j][0]]
                else:
                    dict3[name_list[n]][j].extend(dict2[j])

        for i in dict3.keys():
            dict_list = sorted(dict3[i])
            for k in dict_list:
                dict_temp = {k: dict3[i][k]}
                row = json.dumps(dict_temp)
                hdfs.NameNode('write', row=row, file_name=file_name + '_' + i + '_' + temp, JSON=True)

    def Copy(self, file_name, name_list):
        for i in name_list:
            buffer = StringIO()
            block_original = hdfs.namenode_dict[file_name + '_' + i + '_map']
            for j in block_original:
                name_num, block_num = j // 10, j % 10
                with open('./server/NameNode' + str(name_num) + '/' + str(block_num) +
                          '/' + file_name + '_' + i + '_map' + '.json', 'r') as json_file:
                    for line in json_file:
                        dict_temp = json.loads(line)
                        buffer.write(json.dumps(dict_temp) + '\n')
                        if len(buffer.getvalue()) >= 2000:
                            buffer.seek(0)
                            self.Shuffle(buffer, file_name, [i], temp='reduce')
                            buffer.close()
                            buffer = StringIO()
            buffer.seek(0)
            self.Shuffle(buffer, file_name, [i], temp='reduce')
            buffer.close()

    def Reduce(self, file_name, name_list):
        dict4 = {}
        self.Copy(file_name, name_list)
        for i in name_list:
            dict4[i] = {}
            block_original = hdfs.namenode_dict[file_name + '_' + i + '_reduce']
            count = 0
            for j in block_original:
                name_num, block_num = j // 10, j % 10
                with open('./server/NameNode' + str(name_num) + '/' + str(block_num) +
                          '/' + file_name + '_' + i + '_reduce' + '.json', 'r') as json_file:
                    for line in json_file:
                        dict_temp = json.loads(line)
                        for k in dict_temp.keys():
                            count += sum(dict_temp[k])
                            if k not in dict4[i].keys():
                                dict4[i][k] = sum(dict_temp[k])
                            else:
                                dict4[i][k] += sum(dict_temp[k])
            dict4[i] = dict(sorted(dict4[i].items(), key=lambda item:item[1]))
            for k, v in dict4[i].items():
                dict_temp = {k: v}
                row = json.dumps(dict_temp)
                hdfs.NameNode('write', row=row, file_name=file_name + '_' + i + '_mapreduce', JSON=True)

mapreduce = MapReduce()

