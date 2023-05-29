from io import StringIO
import json
import HDFS
import csv

hdfs = HDFS.hdfs


class MapReduce:
    def __init__(self):
        pass

    def Split(self, block_dict, name_list):        # The beginning of the Map task
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
                    dict1 = {k1: v1}             # read the file from seperated datablocks and split each row to <k,[v]>
                    dict2 = self.Map(dict1)      # call map function to count words
                    cache.write(json.dumps(dict2) + '\n')   # write into cache
                    if len(cache.getvalue()) >= 2000:        # b default, cache size is 2500, the wrote value spill
                        self.Spill(cache, file_name, name_list, combine=True, temp='map') # when size over 80 % of cache
                        cache = StringIO()
        self.Spill(cache, file_name, name_list, combine=True, temp='map')  # spill rest of the cache

    def Spill(self, cache, file_name, name_list, combine=False, temp=''):
        cache.seek(0)                                # spill value will put into shuffle function
        self.Shuffle(cache, file_name, name_list, combine=combine, temp=temp)
        cache.close()                               # clear the cache

    def Map(self, dict1):
        dict2 = {}
        for i in list(dict1.values())[0]:         # map function count every word to [1] in each row,
            dict2[i] = [1]                       # include word count before
        return dict2

    def Shuffle(self, cache, file_name, name_list, combine=False, temp=''):
        dict_shuffle = {}
        for line in cache.readlines():
            dict_load = json.loads(line)                   # read the cache value
            for i in name_list:
                if i not in dict_shuffle.keys():
                    dict_shuffle[i] = {}
            for n, j in enumerate(list(dict_load.keys())):        # partition according to the column
                if j not in dict_shuffle[name_list[n]].keys():
                    dict_shuffle[name_list[n]][j] = dict_load[j]
                elif combine:                                    # combine or merge([n], [n1, n2, ...])
                    dict_shuffle[name_list[n]][j] = [dict_shuffle[name_list[n]][j][0] + dict_load[j][0]]
                else:
                    dict_shuffle[name_list[n]][j].extend(dict_load[j])
        self.write_hdfs(dict_shuffle, file_name, temp=temp)         # call sort and write function

    def write_hdfs(self, dict_write, file_name, temp='', JSON=True):
        for i in dict_write.keys():
            dict_list = sorted(dict_write[i])            # by default sort the dictionary by keys
            for k in dict_list:
                dict_temp = {k: dict_write[i][k]}           # write the results into datablocks row by row in dict type
                hdfs.NameNode('write', row=dict_temp, file_name=file_name + '_' + i + '_' + temp, JSON=JSON)

    def Copy(self, file_name, name_list):               # The beginning of the reduce task
        for i in name_list:
            buffer = StringIO()
            block_original = hdfs.namenode_dict[file_name + '_' + i + '_map']
            for j in block_original:
                name_num, block_num = j // 10, j % 10
                with open('./server/NameNode' + str(name_num) + '/' + str(block_num) +
                          '/' + file_name + '_' + i + '_map' + '.json', 'r') as json_file:
                    for line in json_file:
                        dict_temp = json.loads(line)
                        buffer.write(json.dumps(dict_temp) + '\n')         # write the map results into cache
                        if len(buffer.getvalue()) >= 2000:             # ordering by partition
                            self.Spill(buffer, file_name, [i], temp='reduce')   # spill the middle results
                            buffer = StringIO()                                            # into local storage
            self.Spill(buffer, file_name, [i], temp='reduce')                   # spill rest of the middle results

    def Reduce(self, file_name, name_list):
        dict4 = {}
        self.Copy(file_name, name_list)              # call the copy function
        for i in name_list:
            dict4[i] = {}
            block_original = hdfs.namenode_dict[file_name + '_' + i + '_reduce']
            for j in block_original:
                name_num, block_num = j // 10, j % 10
                with open('./server/NameNode' + str(name_num) + '/' + str(block_num) +
                          '/' + file_name + '_' + i + '_reduce' + '.json', 'r') as json_file:
                    for line in json_file:
                        dict_temp = json.loads(line)
                        for k in dict_temp.keys():
                            if k not in dict4[i].keys():
                                dict4[i][k] = sum(dict_temp[k])           # the reduce function count all the number
                            else:                                    # in dictionary value list and return <word, value>
                                dict4[i][k] += sum(dict_temp[k])          # not the [v], the value is itself
            dict4[i] = dict(sorted(dict4[i].items(), key=lambda item:item[1]))  # This time sort the dictionary
            for k, v in dict4[i].items():                                   # according to the value in ascending order
                dict_temp = {k: v}      # write the final mapreduce dictionary to HDFS row by row
                hdfs.NameNode('write', row=dict_temp, file_name=file_name + '_' + i + '_mapreduce', JSON=True)


mapreduce = MapReduce()
