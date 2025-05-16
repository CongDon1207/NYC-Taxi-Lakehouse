## 🏢 Spark on YARN Architecture Client Mode
  ![image](https://github.com/user-attachments/assets/f0823592-32f3-43a9-9f48-9b5456fb685c)

## 🚀 **Installation Guide**  

#### **Step 1: Clone the Repository**  
```sh
  git https://github.com/huy-dataguy/NYC-Taxi-Lakehouse.git
  cd NYC-Taxi-Lakehouse/Hadoop-Cluster/
```

#### **Step 2: Build Docker Images**  
Building Docker images is required only for the first time or after making changes in the HadoopSphere directory (such as [modifying the owner name](#-modify-the-owner-name)). Make sure Docker is running before proceeding.

💡 Tip: For best performance and compatibility on Windows, it is highly recommended to run Docker with WSL2 backend.

> **⏳ Note:** The first build may take a few minutes as no cached layers exist.
> **⚠️ If you're using Windows:**  
> You might see errors like `required file not found` when running shell scripts (`.sh`) because Windows uses a different line-ending format.  
> To fix this, convert the script files to Unix format using `dos2unix`.

  ```sh
    dos2unix ./scripts/build-image.sh
    dos2unix ./scripts/run-container.sh
    dos2unix ./scripts/resize-number-worker.sh
  ```

```sh
  ./scripts/build-image.sh
```

#### **Step 3: Start the Cluster**  

```sh
  ./scripts/run-container.sh
```

*By default, this will start a cluster with **1 master and 2 slaves**.*  

To start a cluster with **1 master and 5 slaves**:  
```sh
  ./scripts/run-container.sh 6 
```
  ![image](https://github.com/user-attachments/assets/807d54a8-e3e2-498c-a9e1-07cff615a0eb)

#### **Step 4: Verify the Installation**  

After **Step 3**, you will be inside the **client container's CLI**, so to interact with the cluster you need go inside **master container's CLI** by the way run **ssh quochuy-master** you can change 'quochuy' by your hostname.


💡 **Start the HDFS - YARN services:**  
```sh
  ./scripts/start-hdfs-yarn.sh
```
  ![image](https://github.com/user-attachments/assets/65e9f6c1-082a-4471-8ba2-39a3a99c9585)
  ![livedatanote](https://github.com/user-attachments/assets/b0c60eaf-86a1-4b6d-a939-c3241cc6d699)
  ![yarn](https://github.com/user-attachments/assets/da3a7da5-100b-465f-8d1d-4f57ac9574a5)
  
#### **Step 5: Run a Word Count Test**  
```sh
  ./scripts/word_count.sh
```
This script runs a sample **Word Count** job to ensure that HDFS and YARN are functioning correctly.
  ![word_count](https://github.com/user-attachments/assets/c16ecae4-3717-479c-a5d9-21574de8a3ea)
  ![answordcoutn](https://github.com/user-attachments/assets/05bffc7c-712c-43ef-92c4-1a653cc8cbc7)
🚀 **If the Word Count job runs successfully, your system is fully operational!**

After that you need comeback **client container's CLI** by the way **ssh quochuy-client** or **ctrl + d** to continue step 6.
#### **Step 6: Run Spark Submit on Yarn Client Mode** 

Create folder store spark logs
```sh
  hdfs dfs -mkdir /spark-logs
```
Run spark on yarn
```sh
  spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master yarn \
  --deploy-mode client \
  --executor-memory 1G \
  --num-executors 2 \
  $SPARK_HOME/examples/jars/spark-examples_*.jar 10
```
If success you will see answear **Pi = 3,14159**
  

---

## **📌 Important Notes on Volumes & Containers**  
Since the system uses **Docker Volumes** for **NameNode and DataNode**, please ensure that:

- **The number of containers remains the same when restarting** (e.g., if started with 5 slaves, restart with 5 slaves).
- If the number of slaves changes, you may face volume inconsistencies.

✅ **How to Ensure the Correct Number of Containers During Restart**:
1. **Always restart with the same number of containers**:
```sh
  ./scripts/run-container.sh 6  # If you previously used 6 nodes
```

2. **Do not delete volumes when stopping the cluster.**  
> If you just want to stop the cluster (without removing containers), use:

```sh
  docker compose -f compose-dynamic.yaml stop
```

> If you want to remove the containers but **keep the volumes**, use:

```sh
  docker compose -f compose-dynamic.yaml down
```
> Avoid using `docker compose -f compose-dynamic.yaml down -v` as it will remove all container and volumes data.

✅ **Check Existing Volumes**:
```sh
docker volume ls 
```
  ![image](https://github.com/user-attachments/assets/e4813531-94ee-463e-86f9-df9c5987a156)

---

## 🔄 **Modify the Owner Name**  
If you need to change the owner name, run the `rename-owner.py` script and enter your new owner name when prompted.  

> **⏳ Note:** If you want to check the current owner name, it is stored in `hamu-config.json`.
>
> 📌 There are some limitations; you should use a name that is different from words related to the 'Hadoop' or 'Docker' syntax. For example, avoid names like 'hdfs', 'yarn', 'container', or 'docker-compose'.

```sh
python rename-owner.py
```
---

## 🌐 Interact with the Web UI  

You can access the following web interfaces to monitor and manage your Hadoop cluster:  

- **YARN Resource Manager UI** → [http://localhost:9004](http://localhost:9004)  
  Provides an overview of cluster resource usage, running applications, and job details.  

- **NameNode UI** → [http://localhost:9870](http://localhost:9870)  
  Displays HDFS file system details, block distribution, and overall health status.
---
## 📚 Reference Materials

- **Spark Default Configuration**  
  - [IBM Docs](https://www.ibm.com/docs/en/pasc/1.1.1?topic=files-spark-defaultsconf)  
  - [Apache Spark Documentation](https://spark.apache.org/docs/latest/configuration.html)  
  - [Spark Application Submission Guide](https://spark.apache.org/docs/latest/submitting-applications.html)  
  - [Cloudera Sample Applications](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/running-spark-applications/topics/spark-run-sample-apps.html)  

---

## 📞 **Contact**  
📧 Email: [quochuy.working@gmail.com](mailto:quochuy.working@gmail.com)

💬 Feel free to contribute and improve this project! 🚀
