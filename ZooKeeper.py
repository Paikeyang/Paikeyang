import HDFS
import json
import shutil
import os

hdfs = HDFS.hdfs

class ZooKeeper():
    def __init__(self):
        self.namenode_dict = hdfs.namenode_dict.copy()
        if os.path.exists('namenode_copy.json'):
            f = open('namenode_copy.json', 'r')
            self.namenode_dict_copy = json.loads(f.read())
        else:
            self.namenode_dict_copy = {}

    def copy_nodes(self):
        for i in self.namenode_dict.keys():
            for j in self.namenode_dict[i]:
                name_num, block_num = j // 10, j % 10
                name_num_copy, block_num_copy = hdfs.NameNode('check')

                if os.path.exists('./server/NameNode' + str(name_num_copy)) == False:
                    os.makedirs('./server/NameNode' + str(name_num_copy))
                if os.path.exists('./server/NameNode' + str(name_num_copy) + '/' + str(block_num_copy)) == False:
                    os.makedirs('./server/NameNode' + str(name_num_copy) + '/' + str(block_num_copy))

                file = ''
                for _, _, k in os.walk('./server/NameNode' + str(name_num) + '/' + str(block_num)):
                    file = k[0]
                file_name_copy = file[: -4] + '_copy'
                shutil.copyfile('./server/NameNode' + str(name_num) + '/' + str(block_num) + '/' + file,
                                './server/NameNode' + str(name_num_copy) + '/' + str(block_num_copy) + '/'
                                + file_name_copy + file[-4:])

                if file_name_copy not in self.namenode_dict_copy.keys():
                    self.namenode_dict_copy[file_name_copy] = [int(str(name_num_copy) + str(block_num_copy))]
                elif int(str(name_num_copy) + str(block_num_copy)) not in self.namenode_dict_copy[file_name_copy]:
                    self.namenode_dict_copy[file_name_copy].append(int(str(name_num_copy) + str(block_num_copy)))

        namenode_copy_json = json.dumps(self.namenode_dict_copy)
        f = open('namenode_copy.json', 'w')
        f.write(namenode_copy_json)

    def shift(self):
        namenode_copy_json = json.dumps(self.namenode_dict_copy)
        f = open('namenode.json', 'w')
        f.write(namenode_copy_json)
        return self.namenode_dict_copy

zookeeper = ZooKeeper()