import HDFS
import json
import shutil
import os

hdfs = HDFS.hdfs


class ZooKeeper():
    def __init__(self):
        if os.path.exists('namenode_copy.json'):
            f = open('namenode_copy.json', 'r')                   # similar to HDFS, ZooKeeper maintain a namenode_copy
            self.namenode_dict_copy = json.loads(f.read())         # which records copy file name and location
        else:
            self.namenode_dict_copy = {}              # load the namenode_copy if it already exists

    def copy_nodes(self):
        namenode_dict = hdfs.namenode_dict              # copy files according to the HDFS's namenode_dict
        for i in namenode_dict.keys():
            for j in namenode_dict[i]:
                name_num, block_num = j // 10, j % 10
                name_num_copy, block_num_copy = hdfs.NameNode('check')
                hdfs.make_dirs(name_num_copy, block_num_copy)     # create the directory if needed
                file = ''
                for _, _, k in os.walk('./server/NameNode' + str(name_num) + '/' + str(block_num)):
                    file = k[0]
                file_name_copy = file[: -4] + '_copy'
                shutil.copyfile('./server/NameNode' + str(name_num) + '/' + str(block_num) + '/' + file,
                                './server/NameNode' + str(name_num_copy) + '/' + str(block_num_copy) + '/'
                                + file_name_copy + file[-4:])           # copy all files to new datablocks

                self.namenode_dict_copy = hdfs.update_namenode_dict(file_name_copy, self.namenode_dict_copy,
                                                                    name_num_copy, block_num_copy)
        hdfs.json_write('namenode_copy.json', self.namenode_dict_copy)             # update and save namenode_dict_copy

    def shift(self):                                                    # when needed, switch all nodes
        hdfs.json_write('namenode.json', self.namenode_dict_copy)         # in HDFS to ZooKeeper's copy one
        return self.namenode_dict_copy                           # new namenode_dict will replace by namenode_dict_copy


zookeeper = ZooKeeper()