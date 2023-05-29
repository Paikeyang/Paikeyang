Introduction:

This prototype aims to create a MapReduce-like system to identify the passenger with the most flights. 
 
Prototype architecture:

The goal of this prototype is to develop a MapReduce-like system consisting of several essential components: 
HDFS, JobTracker, ZooKeeper, and the MapReduce system. 
  
While the focus is on MapReduce, the other components fulfill their tasks within the MapReduce process.


Prototype produce:

In the Client part, before starting, all file paths and names for the target files are created. 
The Client checks if the target file records exist in the HDFS's NameNode dict. 

If exist, it means the code has run before and an exception is raised, 
then Client directly prints the passenger with the highest number of flights based on the HDFS's NameNode dict. 

  If not exist, the Client proceeds with the following steps: 
upload test files, upload target files, and call JobTracker to execute MapReduce tasks. 

  After completion, ZooKeeper is called to copy all files and nodes. 

Conclusion:

In conclusion, this prototype identifies the passenger with the highest number of flights as UES9151GS5, who took a total of 25 flights.
