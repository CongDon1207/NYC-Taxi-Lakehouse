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

#### **Step 4: Verify the Installation**  

After **Step 3**, you will be inside the **master container's CLI**, where you can interact with the cluster.


💡 **Start the HDFS - YARN services:**  
```sh
  ./scripts/start-hdfs-yarn.sh
```

#### **Step 5: Run a Word Count Test**  
```sh
  ./scripts/word_count.sh
```
This script runs a sample **Word Count** job to ensure that HDFS and YARN are functioning correctly.

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


🚀 **If the Word Count job runs successfully, your system is fully operational!**

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

### 🌐 Interact with the Web UI  

You can access the following web interfaces to monitor and manage your Hadoop cluster:  

- **YARN Resource Manager UI** → [http://localhost:9004](http://localhost:9004)  
  Provides an overview of cluster resource usage, running applications, and job details.  

- **NameNode UI** → [http://localhost:9870](http://localhost:9870)  
  Displays HDFS file system details, block distribution, and overall health status.

---

## 📞 **Contact**  
📧 Email: quochuy.working@gmail.com  

💬 Feel free to contribute and improve this project! 🚀