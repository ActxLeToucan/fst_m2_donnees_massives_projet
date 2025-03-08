# projet
## 1. Jobs et tâches
### 1. Liste des tâches
> [!NOTE]
> TODO

### 2. Liste des jobs
```sh
hadoop com.sun.tools.javac.Main */*.java
jar -cf proj.jar */*.class
hadoop jar proj.jar driver.JobList -D mapreduce.job.reduces=2 -fs file:/// -jt local <input> <output>
```

## 2. Pic de consommation
```sh
hadoop com.sun.tools.javac.Main */*.java
jar -cf proj.jar */*.class
hadoop jar proj.jar driver.Pic -D mapreduce.job.reduces=2 -fs file:/// -jt local <input> <output> [-max]
```
On peut ajouter l'option `-max` travailler à partir du max de consommation des instances au lieu de la moyenne.

## 3. Estimation de la puissance de chaque machine
```sh
hadoop com.sun.tools.javac.Main */*.java
jar -cf proj.jar */*.class
hadoop jar proj.jar driver.Puisssance -D mapreduce.job.reduces=2 -fs file:/// -jt local <input> <output>
```
