import HDFS
import JobTracker
import ZooKeeper

hdfs = HDFS.hdfs
jobtracker = JobTracker.jobtracker
zookeeper = ZooKeeper.zookeeper

namenode_dict = hdfs.namenode_dict
file_path = ['./data/AComp_Passenger_data_no_error.csv', './data/Top30_airports_LatLong.csv']
file_name = ['AComp_Passenger', 'airports_LatLong']
name_list = [['Passenger_id', 'From_airport_code', 'Destination_airport_code',
                       'Departure_time', 'Total_Unix_epoch_time', 'Total_mins'],
             ['Airport_Name', 'Airport_code', 'Latitude', 'Longitude']]

run = True
for i in range(len(file_path)):
    run = run and (file_name[i] not in namenode_dict.keys())
if run == True:
    hdfs.upload_file('./data/AComp_Passenger_data.csv', 'AComp_Passenger_test')
    hdfs.upload_file('./data/AComp_Passenger_data_no_error_DateTime.csv', 'AComp_Passenger_test')

def Client(path, name, list):
    for i in range(len(path)):
        hdfs.upload_file(path[i], name[i])
        block_dict = {name[i]: namenode_dict[name[i]]}
        jobtracker.Parallelization(block_dict, list[i])
    zookeeper.copy_nodes()

if __name__ == '__main__':

    if run:
        Client(file_path, file_name, name_list)
        print('Passenger who has the highest number of flights: ',
              hdfs.NameNode('read',file_name=file_name[0] + '_' + name_list[0][0] + '_mapreduce' , JSON=True)[-1])
    else:
        print('Passenger who has the highest number of flights: ',
              hdfs.NameNode('read', file_name=file_name[0] + '_' + name_list[0][0] + '_mapreduce', JSON=True)[-1])
        raise Exception('Please delete all directories under ./server, file namenode.json '
                        'and file namenode_copy.json before start')
