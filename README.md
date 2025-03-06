# projet
## 1
### 2. Liste des jobs
```sh
hadoop com.sun.tools.javac.Main */*.java
jar -cf proj_1.2.jar */*.class
hadoop jar proj_1.2.jar driver.JobList -D mapreduce.job.reduces=2 -fs file:/// -jt local <input> <output>
```