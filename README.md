# projet
## 1. Jobs et tâches
### 1. Liste des tâches
```sh
hadoop com.sun.tools.javac.Main */*.java
jar -cf proj.jar */*.class
hadoop jar proj.jar driver.TaskList -D mapreduce.job.reduces=2 -fs file:/// -jt local <input> <output>
```

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

## 4. Clusterisation en utilisation l'algorithme K-Means
```sh
hadoop com.sun.tools.javac.Main */*.java
jar -cf proj.jar */*.class
hadoop jar proj.jar driver.KMean -D mapreduce.job.reduces=1 -fs file:/// -jt local <input> <output> <centroids_file> [-iter <nb_iterations>]
```

L'option `-iter` permet de spécifier le nombre d'itérations maximum de l'algorithme K-Means (par défaut 10).
L'entrée centroids_file est un fichier contenant les coordonnées des centroids initiaux (nous n'avons pas implémenté la génération automatique des centroids initiaux). Un exemple de ce fichier est disponible à la racine du projet : `/centroids.txt`.