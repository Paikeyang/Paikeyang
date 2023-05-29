from threading import Thread
import MapReduce
import HDFS
import random

hdfs = HDFS.hdfs
mapreduce = MapReduce.mapreduce


class JobTracker():
    def __init__(self):
        self.target_map = mapreduce.Split         # initial two main task Map and Reduce
        self.target_reduce = mapreduce.Reduce

    def Parallelization(self, block_dict, name_list):
        file_name, block = list(block_dict.keys())[0], list(block_dict.values())[0]
        t1, t2, t3 = self.Track_Map(block, name_list, file_name)     # create 3 thread doing Map task
        self.run_thread(t1, t2, t3)                                  # run 3 thread
        t1, t2, t3 = self.Track_Reduce(file_name, name_list)          # create 3 thread doing Reduce task
        self.run_thread(t1, t2, t3)                                  # run 3 thread

    def Track_Map(self, block, name_list, file_name):
        random.shuffle(block)                            # equally separate the task into 3 through the length of block
        partition = int(len(block) / 3)
        block1, block2, block3 = block[: partition], block[partition: 2 * partition], block[2 * partition:]
        block_dict1, block_dict2, block_dict3 = {file_name: block1}, {file_name: block2}, {file_name: block3}
        t1 = Thread(target=self.target_map(block_dict=block_dict1, name_list=name_list))
        t2 = Thread(target=self.target_map(block_dict=block_dict2, name_list=name_list))   # create three thread
        t3 = Thread(target=self.target_map(block_dict=block_dict3, name_list=name_list))
        return t1, t2, t3

    def Track_Reduce(self, file_name, name_list):
        division = int(len(name_list) / 3)        # equally separate the task into 3 through the length of name_list
        group1, group2, group3 = name_list[: division], name_list[division: 2 * division], name_list[2 * division:]
        t1 = Thread(target=self.target_reduce(file_name, group1))
        t2 = Thread(target=self.target_reduce(file_name, group2))    # create three thread
        t3 = Thread(target=self.target_reduce(file_name, group3))
        return t1, t2, t3

    def run_thread(self, t1, t2, t3):
        t1.start()
        t2.start()
        t3.start()                  # run each thread, the function finished only if all threads complete
        t1.join()
        t2.join()
        t3.join()


jobtracker = JobTracker()