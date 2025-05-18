# Set up Spark in linux

Tutorial: [https://phoenixnap.com/kb/install-spark-on-ubuntu](https://phoenixnap.com/kb/install-spark-on-ubuntu)

# Update system package list

```bash
sudo apt update
```

# Install Spark dependencies (Git, Scala, Java)

```bash
sudo apt install default-jdk scala git -y
```

Verify the installed dependencies

```bash
java -version; javac -version; scala -version; git --version
```

# Download Apache Spark on Ubuntu

Link download Spark 3.5: [https://www.apache.org/dyn/closer.lua/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz](https://www.apache.org/dyn/closer.lua/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz)

## Extract

```bash
tar xvf spark-3.5.5-bin-hadoop3.tgz 
```

## Move the unpacked directory *spark-3.5.5-bin-hadoop3* to the */usr/local/spark* directory

```bash
 sudo mv spark-3.5.5-bin-hadoop3 /usr/local/spark
```

## Check installed

```bash
/usr/local/spark/bin/spark-shell --version
```

# Configuration

```bash
nano ~/.bashrc

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=/usr/bin/python3

```

# Start Standalone Spark Master Server

```bash
start-master.sh
```