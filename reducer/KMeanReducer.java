package reducer;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import java.util.*;

import javax.naming.Context;

public class KMeanReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double[] sumPoint = new double[]{
            0,
            0,
            //0,
            0
            //0
        };
        int count = 0;

        for (Text value : values) {

            String[] fields = value.toString().split(",");
        
            double startTime = Double.parseDouble(fields[4]);
            double endTime = Double.parseDouble(fields[5]);
            double moyCpu = Double.parseDouble(fields[7]);
            //double maxCpu = Double.parseDouble(fields[8]);
            double moyRam = Double.parseDouble(fields[9]);
            //double maxRam = Double.parseDouble(fields[10]);
            
            double duration = endTime - startTime;

            double[] point = new double[]{
                duration,
                moyCpu,
                //maxCpu,
                moyRam
                //maxRam
            };
            
            for(int i = 0; i < point.length; i++)
                sumPoint[i] = point[i];
            
            count++;
        }


        double newCentroidDuration = sumPoint[0] / count;
        double newCentroidMoyCpu = sumPoint[1] / count;
       // double newCentroidMaxCpu = sumPoint[2] / count;
        double newCentroidMoyRam = sumPoint[2] / count;
        //double newCentroidMaxRam = sumPoint[4] / count;
        

        // Écriture de la sortie : <index_centroid> <duration> ; <moyCpu> ; <maxCpu> ; <MoyRam> ; <MaxRam>
        //context.write(key, new Text(newCentroidDuration + "," + newCentroidMoyCpu + "," + newCentroidMaxCpu + "," + newCentroidMoyRam + "," + newCentroidMaxRam));
        context.write(key, new Text(newCentroidDuration + "," + newCentroidMoyCpu + "," + newCentroidMoyRam));
    }
}

