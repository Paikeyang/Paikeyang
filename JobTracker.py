from threading import Thread
import MapReduce
import HDFS
import random

hdfs = HDFS.hdfs
mapreduce = MapReduce.mapreduce

class JobTracker():
    def __init__(self):
        self.target_map = mapreduce.Split
        self.target_reduce = mapreduce.Reduce

    def Parallelization(self, block_dict, name_list):
        file_name, block = list(block_dict.keys())[0], list(block_dict.values())[0]
        random.shuffle(block)
        partition = int(len(block) / 3)
        block1, block2, block3 = block[: partition], block[partition : 2 * partition], block[2 * partition:]
        block_dict1, block_dict2, block_dict3 = {file_name: block1}, {file_name: block2}, {file_name: block3}
        t1 = Thread(target=self.target_map(block_dict=block_dict1 ,name_list=name_list))
        t2 = Thread(target=self.target_map(block_dict=block_dict2, name_list=name_list))
        t3 = Thread(target=self.target_map(block_dict=block_dict3, name_list=name_list))
        t1.start()
        t2.start()
        t3.start()
        t1.join()
        t2.join()
        t3.join()
        division = int(len(name_list) / 3)
        group1, group2, group3 = name_list[: division], name_list[division: 2 * division], name_list[2 * division:]
        t1 = Thread(target=self.target_reduce(file_name, group1))
        t2 = Thread(target=self.target_reduce(file_name, group2))
        t3 = Thread(target=self.target_reduce(file_name, group3))
        t1.start()
        t2.start()
        t3.start()
        t1.join()
        t2.join()
        t3.join()

jobtracker = JobTracker()