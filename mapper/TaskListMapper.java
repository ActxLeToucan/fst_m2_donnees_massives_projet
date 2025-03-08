package mapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class TaskListMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        // Récupération de la ligne CSV
        String[] fields = value.toString().split(",");

        String jobName = fields[1]; 
        String taskName = fields[2];
        String startTime = fields[4]; // Date de départ
        String endTime = fields[5];   // Date de fin
        
        // Vérification des données valides
        if (!startTime.isEmpty() && !endTime.isEmpty()) {
            try {
                context.write(new Text(jobName + " -> " + taskName + " : "), new Text(startTime + ";" + endTime + ";1")); // "1" pour compter le nombre d'instances
            } catch (NumberFormatException e) {
                // Ignore les lignes mal formées
            }
        }
    }
}
