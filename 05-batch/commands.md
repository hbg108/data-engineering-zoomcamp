# 05-batch

## Installing Java

```bash
cd
mkdir spark
cd spark/
wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz
rm openjdk-11.0.2_linux-x64_bin.tar.gz
export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"
which java
java --version
```

## Installing Spark

```bash
wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
tar xzfv spark-3.3.2-bin-hadoop3.tgz
rm spark-3.3.2-bin-hadoop3.tgz
export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
```

## Testing Spark

```bash
spark-shell
```

```scala
val data = 1 to 10000
val distData = sc.parallelize(data)
distData.filter(_ < 10).collect()
```

```bash
:quit
```

## Setting Environment Variables

```bash
cd
nano .bashrc

export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"

export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"

Ctrl + O
Enter
Crtl + X

source .bashrc
```

## PySpark - Using Conda

```bash
# cd
# nano .bashrc

# export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
# export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"

# source .bashrc

conda install -c conda-forge pyspark

# export PYSPARK_PYTHON="/home/hbg/anaconda3/bin/python"
# export PYSPARK_DRIVER_PYTHON="/home/hbg/anaconda3/bin/python"

wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

## PySpark

```bash
conda deactivate

# export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
# export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"

sudo apt-get install python3-pip
sudo apt-get install python3-testresources
/usr/bin/python3 -m pip install ipykernel -U --user --force-reinstall

export PATH="/home/hbg/.local/bin:${PATH}"
```

## 04_pyspark.ipynb

```bash
conda deactivate
/usr/bin/python3 -m pip install pandas
```

<span style="color:red">AttributeError</span>: 'DataFrame' object has no attribute 'iteritems'

```bash
/usr/bin/python3 -m pip install -U pandas==1.5.3
```