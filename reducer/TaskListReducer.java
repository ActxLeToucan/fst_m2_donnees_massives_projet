package reducer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import java.util.*;

public class TaskListReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long minStart = Long.MAX_VALUE;
        long maxEnd = 0;

        long instanceCount = 0;

        Long totalDuration = 0l;
        List<Long> durations = new ArrayList<>();

        for (Text value : values) {
            String[] parts = value.toString().split(";");
            Long startTime = Long.parseLong(parts[0]);
            Long endTime = Long.parseLong(parts[1]);
            Long count = Long.parseLong(parts[2]);

            // On passe si les dates de début ou de fin ne sont pas cohérentes
            if(endTime <= 0 || startTime < 0 || startTime > endTime){
                continue;
            }

            minStart = Math.min(minStart, startTime);
            maxEnd = Math.max(maxEnd, endTime);

            long duration = endTime-startTime;
            durations.add(duration);
            totalDuration += duration;

            instanceCount += count;
        }

        // On passe si la date de début ou de fin de la tâche ne sont pas cohérentes
        if(maxEnd < minStart) return;

        // Calcul de la durée moyenne
        double averageDuration = (double) totalDuration / instanceCount;

        // Calcul des stragglers
        long stragglerCount = 0;
        double stragglerThreshold = 1.2 * averageDuration;
        for (long duration : durations) {
            if (duration > stragglerThreshold) {
                stragglerCount++;
            }
        }
        
        Long taskDuration = maxEnd - minStart;

        // Écriture de la sortie : <nomTâche> <durée de la tâche> ; <nombre d'instances> ; <nombre de stragglers>
        context.write(key, new Text(taskDuration + " ; " + instanceCount + " ; " + stragglerCount));
    }
}

