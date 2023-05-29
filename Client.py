import HDFS
import JobTracker
import ZooKeeper

hdfs = HDFS.hdfs
jobtracker = JobTracker.jobtracker
zookeeper = ZooKeeper.zookeeper

namenode_dict = hdfs.namenode_dict
file_path = ['./data/AComp_Passenger_data_no_error.csv', './data/Top30_airports_LatLong.csv']
file_name = ['AComp_Passenger', 'airports_LatLong']                   # Predefine path, file name and attribute names
name_list = [['Passenger_id', 'From_airport_code', 'Destination_airport_code',
                       'Departure_time', 'Total_Unix_epoch_time', 'Total_mins'],
             ['Airport_Name', 'Airport_code', 'Latitude', 'Longitude']]

run = True
for i in range(len(file_path)):
    run = run and (file_name[i] not in namenode_dict.keys())    # Check whether the file names are in namenode records
if run:                                       # Code only run when it never runs before.
    hdfs.upload_file('./data/AComp_Passenger_data.csv', 'AComp_Passenger_test')
    hdfs.upload_file('./data/AComp_Passenger_data_no_error_DateTime.csv', 'AComp_Passenger_test')
                                                      # Upload test files, which will not use to analyse


def Client(path, name, name_list):
    for i in range(len(path)):                       # Client by sequential ask hdfs uploads target files,
        hdfs.upload_file(path[i], name[i])            # and call Jobtracker do parallelization mapreduce tasks
        block_dict = {name[i]: namenode_dict[name[i]]}
        jobtracker.Parallelization(block_dict, name_list[i])
    zookeeper.copy_nodes()                        # After all tasks done, call ZooKeeper to copy files.


if __name__ == '__main__':
    if run:
        Client(file_path, file_name, name_list)              # After MapReduce, print the final results according to
        print('Passenger who has the highest number of flights: ',             # the namenode records, which are sorted
              hdfs.NameNode('read',file_name=file_name[0] + '_' + name_list[0][0] + '_mapreduce' , JSON=True)[-1])
    else:
        print('Passenger who has the highest number of flights: ',
              hdfs.NameNode('read', file_name=file_name[0] + '_' + name_list[0][0] + '_mapreduce', JSON=True)[-1])
        raise Exception('Please delete all directories under ./server, file namenode.json '
                        'and file namenode_copy.json before start')      # Project wouldn't run if it runs before!
