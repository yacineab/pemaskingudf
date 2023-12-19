---

# Project Overview

Welcome to our project! Our aim is to enhance Hive's User-Defined Function (UDF) capabilities, specifically focusing on the "Masking Hash" UDF and extending its support to include numeric data types. This document serves as a comprehensive guide to understanding, building, and utilizing our project effectively.

## Supported Types
The current version supports the following data types: **STRING, VARCHAR, CHAR, INT, BIGINT**.

## Environment Versions

Artifact | Version
------- | -------
**CDP Version** | 7.1.8
**Java** | 11
**Hadoop** | 3.1.1.7.1.8.0-801
**Hive** | 3.1.3000.7.1.8.0-801

---
# Features
- **String Data Types**: Hashing of **STRING, VARCHAR, CHAR** using the *sha256Hex* algorithm.
- **Numeric Data Types**: Hashing of **Int, BigInt** using the standard multiplicative hashing algorithm.

---
# Getting Started
## Prerequisites
- JDK 11
- Apache Maven
- Access to Hadoop and Hive environments

## Building the Project
To build the project, run:
```shell
mvn clean install
```

### Running Unit Tests
Execute unit tests with:
```shell
mvn test
```

---

# Using the UDF
## Deploying the JAR in HDFS
### Direct Reference Method

Upload the JAR to the HDFS path where UDFs are stored. For instance:
```shell
hdfs dfs -put universalhashudf-CDP7.1.5-version-jar-with-dependencies.jar /hdfs/udfpath/
```

## Registering the UDF

## Registering the UDF
1. Connect to HiveServer using Beeline with a user having **UDF** access.
2. Select the database:
   ```shell
   use default;
   ```
3. Register the UDF, adjusting the command based on your cluster configuration:
   ```shell
   CREATE FUNCTION universalhashudf AS 'UniversalHashUDF' USING JAR 'hdfs:///udf/hdfspath/universalhashudf-CDP7.1.5-version-jar-with-dependencies.jar';
   ```
   > If your configuration differs, adjust the JAR location accordingly.

4. Verify the UDF registration:
   ```shell
   SHOW FUNCTIONS like "%hash%";
   ```
   
## Example Usage
### Query with Hashing UDF

Let's assume we have the following table:
```
+---------------+------------------+---------------+---------------------+
| person.name   | person.firstname | person.age    |    person.phone     |
+---------------+------------------+---------------+---------------------+
| ab            | ya               | 10            | 111                 |
| aze           | qsd              | 15            | 333                 |
| dd            | aa               | 111           | 10                  |
| dds           | ava              | 333           | 15                  |
| pre           | nm               | 99            | 999988877755443321  |
+---------------+------------------+---------------+---------------------+
```

Execute the query:

```sql
SELECT 
    default.universalhashudf(person.name), 
    firstname, 
    default.universalhashudf(person.age), 
    default.universalhashudf(person.phone) 
FROM 
    peudf.person;
```

**Result**
```
+-------------------------+-----------+--------------+-----------------------+
|          _c0            | firstname |     _c2      |          _c3          |
+-------------------------+-----------+--------------+-----------------------+
| fb8e20fc2e4c3f248c60... | ya        | -383449968   | -1186877602           |
| 9adfb0a6d03beb7141d8... | qsd       | -870655931   | -1409135997           |
| 9b7ecc6eeb83abf9ade1... | aa        | -1186877602  | -383449968            |
| f79fe6c5dd8b31ff18ae... | ava       | -1409135997  | -870655931            |
| 695252f664b93b2375fc... | nm        | -1672646182  | -9185032973038813827  |
+-------------------------+-----------+--------------+-----------------------+
```

---
# Advanced Configuration
## Ranger Tag-Based Masking Policies
1. Create a new policy in the Ranger interface.
2. Define the tag for the policy.
3. Select the applicable role/group/user.
4. In 'Policy Conditions', check "select".
5. In 'Component Permissions', select **HIVE**.
6. Under 'Select Masking Option', choose **Custom** and input **universalhashudf({col})**.


> Similar steps apply for **Resource-Based Policies**.

---

# In-Depth: Hashing Algorithm for Integers
Utilizing the MurmurHash Algorithm, we implement an efficient, collision-resistant hashing mechanism.
The MurmurHash Algorithm, created by Austin Appleby in 2008. It's an implementation of the standard multiplicative hashing algorithm.

## Algorithm
```
Ha(Key) = (a * K mod W) / (W / M)
```

The value **a** is an appropriately chosen constant that should be relatively prime to **W**.
It should be large with a binary representation of a random mix of 1s and 0s.
An important practical special case occurs when **W** and **M** are chosen as powers of 2.

The formula then becomes:
```
Ha(Key) = (a * K mod 2^w) / 2^(w-n)
```
This is special because arithmetic modulo 2^w is performed by default in low-level programming languages.
Integer division by a power of 2 is simply a rightward shift, thus in **JAVA**, for instance, this function becomes:
```
Ha(Key) = (a * K) >>> (w-m);
```

> For fixed *m* and *w*, this translates to a single integer multiplication and a rightward shift, making it one of the fastest hash functions to compute.

Multiplicative hashing is susceptible to a "common mistake" leading to poor dispersion - higher-value input bits don't affect lower-value output bits.
To address this, we shift the top-level bits rightward (shift-right) and perform XORs with the key.
The result is:
```
Ha(Key) = {
Key ^= Key >>> (w-m);
return (a * Key) >>> (w-m);
}
```

### Choosing the Constants W and M

> The constants were chosen in the implementation to ensure three points:
> - Avoidance of collisions
> - Ensuring maximum distribution of hashed keys
> - Guaranteeing idempotence

### Int Implementation

```Java
key ^= key >>> 16; 
key *= 0x85ebca6b;
key ^= key >>> 13; 
key *= 0xc2b2ae35;
key ^= key >>> 16; 
return key;
```

### Long Implementation

```Java
key += 0x9e3779b97f4a7c15L;
key ^= (key >>> 30); 
key *= 0xbf58476d1ce4e5b9L;
key ^= (key >>> 27); 
key *= 0x94d049bb133111ebL;
key ^= (key >>> 31);
res = key;
```