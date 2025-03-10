package mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class KMeanMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private List<double[]> centroids = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // Lire les centres des clusters
        String centroidsData = context.getConfiguration().get("centroids");
        for (String line : centroidsData.split("\n")) {
            String[] parts = line.split(",");
            centroids.add(new double[]{
                Double.parseDouble(parts[0]), 
                Double.parseDouble(parts[1]),
                Double.parseDouble(parts[2])
            });
        }

        // Ajout de points artificiels pour assurer la non disparition des centroïdes
        for(int i = 0; i<centroids.size(); i++){
            double[] centroid = centroids.get(i);
            context.write(new IntWritable(i), new Text("1,1,1,1,0," + centroid[0] +",1," + centroid[1] + ",0," + centroid[2] + ",0,0"));
        }
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        // Récupération de la ligne CSV
        String[] fields = value.toString().split(",");

        // Vérification qu'on a l'intégralité des colonnes représentées
        if(fields.length < 11) return;

        // Caractéristiques
        String startTimeStr = fields[4]; // Date de départ
        String endTimeStr = fields[5];   // Date de fin
        String moyCpuStr = fields[7];
        String moyRamStr = fields[9];

        if(startTimeStr.isEmpty() || endTimeStr.isEmpty() || moyCpuStr.isEmpty() || moyRamStr.isEmpty())
            return;
        
        double startTime = Double.parseDouble(startTimeStr);
        double endTime = Double.parseDouble(endTimeStr);
        double moyCpu = Double.parseDouble(moyCpuStr);
        double moyRam = Double.parseDouble(moyRamStr);

        if(startTime > endTime || startTime < 0 || endTime < 0)
            return;
            
        double duration = endTime - startTime;

        double[] point = new double[]{
            duration,
            moyCpu,
            moyRam
        };

        // Trouver cluster le plus proche
        int closestCluster = 0;
        double minDistance = Double.MAX_VALUE;
        for(int i = 0; i < centroids.size(); i++){
            double dist = euclidianDistanceToCentroid(point, centroids.get(i));
            if(dist < minDistance){
                minDistance = dist;
                closestCluster = i;
            }
        }

        // Vérification des données valides
        try {
            context.write(new IntWritable(closestCluster), value);
        } catch (NumberFormatException e) {
            // Ignore les lignes mal formées
        }
    }

    private double euclidianDistanceToCentroid(double[] point, double[] centroid){
        double sum = 0;

        for(int i = 0; i < point.length; i++){
            sum += Math.pow(centroid[i]-point[i], 2);
        }

        return Math.sqrt(sum);
    }
}
