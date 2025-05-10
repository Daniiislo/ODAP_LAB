# Set up Hadoop in linux

References: [GeeksForGeeks - Install Hadoop](https://www.geeksforgeeks.org/how-to-install-hadoop-in-linux/)

## Install JDK for Hadoop

```bash
#check whether the system is equipped with java
java -version

#update system
sudo apt-get update
sudo apt-get install update

#install default jdk for java
sudo apt-get install default-jdk
```

## Create a user for Hadoop

```bash
#Create group user
sudo addgroup hadoop

#Create user in hadoop group
sudo adduser --ingroup hadoop hadoopusr

#Add hadoopusr to sudo group to make it become a superuser
sudo adduser hadoopusr sudo
```

## Install ssh key

```bash
#Install ssh key
sudo apt-get install openssh-server

#Switch to hadoopusr user
su - hadoopusr

#Generate ssh key
ssh-keygen -t rsa -P ""

#add the public key of the computer to the authorized key file of the compute
#that you want to access with ssh keys
cat $HOME/ .ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys

#check for the local host i.e. ssh localhost with below command and
#press yes to continue and enter your password if it ask then type exit.
ssh localhost
```

## Download Hadoop package

Link: [https://archive.apache.org/dist/hadoop/common/hadoop-2.9.0/](https://archive.apache.org/dist/hadoop/common/hadoop-2.9.0/)

Download the .tar.gz

Extract:

```bash
sudo tar xvzf hadoop-2.9.0.tar.gz
```

Change the ownership:

```bash
sudo chown -R hadoopusr /usr/local
```

## Configuration

- .bashrc

```bash
sudo nano ~/.bashrc

#Paste this in the file
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib"

# Note that you must change the java version according to your PC java version

#Ctrl + O => Enter => Ctrl + X to save and exit nano
```

- hadoop-env.sh

```bash
sudo nano /usr/local/hadoop/etc/hadoop/hadoop-env.sh

#Alther this in the JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 #according to your PC java version
```

- core-site.xml

```bash
sudo nano /usr/local/hadoop/etc/hadoop/core-site.xml

#Paste this in the configuration tag
<property>
<name>fs.default.name</name>
<value>hdfs://localhost:9000</value>
</property>
```

- hdfs.xml

```bash
sudo nano /usr/local/hadoop/etc/hadoop/hdfs-site.xml

#Paste this in the configuration tag
<property>
<name>dfs.replication</name>
<value>1</value>
</property>
<property>
<name>dfs.namenode.name.dir</name>
<value>file:/usr/local/hadoop_tmp/hdfs/namenode</value>
</property>
<property>
<name>dfs.datanode.data.dir</name>
<value>file:/usr/local/hadoop_tmp/hdfs/datanode</value>
</property>
```

- yarn-site.xml

```bash
sudo nano /usr/local/hadoop/etc/hadoop/yarn-site.xml

#Paste this in the configuration tag
<property>
<name>yarn.nodemanager.aux-services</name>
<value>mapreduce_shuffle</value>
</property>
<property>
<name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
<value>org.apache.hadoop.mapred.ShuffleHandler</value>
</property>
```

> We have _`mapred-site.xml.template`_ so we need to locate that file then copy this file to that location and then _`rename`_ it.

```bash
sudo cp /usr/local/hadoop/etc/hadoop/mapred-site.xml.template /usr/local/hadoop/etc/hadoop/mapred-site.xml
```

- mapred-site.xml

```bash
sudo nano /usr/local/hadoop/etc/hadoop/mapred-site.xml

#Paste this the configuration tag
<property>
<name>mapreduce.framework.name</name>
<value>yarn</value>
</property>
```

## Create folder for namenode and datanode

```bash
sudo mkdir -p /usr/local/hadoop_space
sudo mkdir -p /usr/local/hadoop_space/hdfs/namenode
sudo mkdir -p /usr/local/hadoop_space/hdfs/datanode
```

- Give permission

```bash
sudo chown -R hadoopusr /usr/local/hadoop_space
```

## Running hadoop

> Ensure that using `hadoopusr` user

- Format the namenode in the first time

```bash
hdfs namenode -format
```

- Start the DFS i.e. Distributed File System.

```bash
start-dfs.sh
```

- Start the yarn

```bash
start-yarn.sh
```

- Using this cmd to check

```bash
jps
```

## Tutorial video

Link:
