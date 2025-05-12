# Map-Side Join in Hadoop (TPC-H Benchmark)

This project demonstrates how to perform a **Map-Side Inner Join** using Hadoop's MapReduce (new API). The goal is to efficiently join the `customer` and `orders` tables from the **TPC-H benchmark** in a scalable fashion using sorted and partitioned input files and `CompositeInputFormat`. The final join is executed fully in the **map phase**, avoiding reducers and shuffle.

---

## 🛠 Technologies Used

* Java 8+
* Hadoop 3.3.5
* MapReduce New API (`org.apache.hadoop.mapreduce.*`)
* HDFS (Hadoop Distributed File System)
* SequenceFile + CompositeInputFormat
* Docker for Hadoop cluster

---

## 📁 Project Structure

```
├── src/main/java/
│   ├── Sorter.java          # Sort and partition input data (customer/orders)
│   ├── Joiner.java # Map-side join using CompositeInputFormat
├── pom.xml                        # Maven build file
```

---

## 📦 How It Works

1. **Prepare Input**

    * Raw `.tbl` files (pipe-delimited) from TPC-H: `customer.tbl`, `orders.tbl`
    * Upload to HDFS (e.g., `/tpch/raw/`)

2. **Sort and Partition**

    * Build Artefacts from `Sorter.java` and send it to docker:
        - `sudo docker cp out/artifacts/Sorter/Sorter.jar namenode:/home/hadoop/sorter.jar`
    * Then run it on both tables: 
      - `$ hadoop jar sorter.jar Sorter /data/raw/customer.tbl /data/sorted/customer 4` or 
      - `$ hadoop jar sorter.jar Sorter /data/raw/orders.tbl /data/sorted/orders 4`
    * Output: would depend on your own directory structure of your HDFS

3. **Map-Side Join**

    * Run `Joiner.java`
    * Uses `CompositeInputFormat` to join both sorted inputs
    * Output: `/tpch/output/join/` (joined records as text)

[//]: # (---)

[//]: # ()
[//]: # (## ▶️ Example Run)

[//]: # ()
[//]: # (```bash)

[//]: # (# Sort)

[//]: # (hadoop jar tpch-joins.jar SortByCustKey /tpch/raw/customer.tbl /tpch/sorted/customer 4)

[//]: # (hadoop jar tpch-joins.jar SortByCustKey /tpch/raw/orders.tbl /tpch/sorted/orders 4)

[//]: # ()
[//]: # (# Join)

[//]: # (hadoop jar tpch-joins.jar CustomerOrderMapSideJoin /tpch/sorted/customer /tpch/sorted/orders /tpch/output/join)

[//]: # (```)

---

## ✅ Output

* Joined records where `orders.custkey = customer.custkey`
* Written as plain text (joined fields) to HDFS
* Output filenames: `part-m-00000`, `part-m-00001`, etc.

---

## 🧠 Notes

* No shuffle or reducer phase is used in the join job
* This is a scalable and efficient method when both inputs are large

---


## 👤 Author

Pedram — Master's Student in Computer Science

If you find this helpful, feel free to ⭐ the repo!
