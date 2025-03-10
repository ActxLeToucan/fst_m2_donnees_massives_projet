package driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Random ;
import reducer.KMeanReducer;
import mapper.KMeanMapper;


public class KMean extends Configured implements Tool {

	private final int MAX_ITERATION = 10;

	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Configuration(), new KMean(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.printf("Usage: KMean [generic options] <input dir> <output dir> <centroids file> [options]\n");
			return -1 ;
		}

		Configuration conf = this.getConf() ;

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Path centroidPath = new Path(args[2]);
		// KMean -> Algorithme Itératif donc on repète en boucle un même job pour "affiner" la position des centroïdes
		for(int i = 0; i < MAX_ITERATION; i++){


			// On récupère le fichier temporaire des centroïdes
			Path pp = i == 0 ? centroidPath : new Path(outputPath, (i-1)+"/centroids.txt");
			conf.set("centroids", readCentroidFile(conf, pp));

			Job job = Job.getInstance(conf);

			job.setJarByClass(KMean.class);
			job.setJobName("KMean");

			FileInputFormat.setInputPaths(job, inputPath);
			FileOutputFormat.setOutputPath(job, new Path(outputPath, ""+i));

			job.setMapperClass(KMeanMapper.class);
			job.setReducerClass(KMeanReducer.class);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);

			boolean success = job.waitForCompletion(true);

			// TODO : vérifier la convergence
			
			if(success){
				Path newCentroidPath = new Path(outputPath, i+"/centroids.txt");
				saveNewCentroids(conf, new Path(outputPath, ""+i), newCentroidPath);
			}else{
				return 1;
			}
		}

		return 0;
	}

	private String readCentroidFile(Configuration conf, Path centroidPath) throws IOException{
		StringBuilder centroidsData = new StringBuilder();
		FileSystem fs = FileSystem.get(conf);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(centroidPath)));
		String line;
		while ((line = reader.readLine()) != null) {
			centroidsData.append(line.split("\t")[1]).append("\n");
		}
		reader.close();
		return centroidsData.toString();
	}

	/**
	 * Sauvegarde les nouveaux centroïdes calculés dans un fichier séparé pour l'itération.
	 *
	 * @param conf             Configuration Hadoop
	 * @param outputPath       Chemin des sorties de l'itération courante
	 * @param centroidsOutput  Chemin du nouveau fichier de centroïdes à générer
	 */
	private void saveNewCentroids(Configuration conf, Path outputPath, Path centroidsOutput) throws IOException {
		FileSystem fs = FileSystem.get(conf);

		// Chemin vers le fichier de résultats intermédisaires (produit par le Reducer)
		Path partFile = new Path(outputPath, "part-r-00000");
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(partFile)));

		// Créer un fichier pour sauvegarder les nouveaux centroïdes
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(centroidsOutput, true)));

		String line;
		while ((line = reader.readLine()) != null) {
			String[] parts = line.split("\t"); // Split sur tabulation (ClusterID et coordonnées)
			if (parts.length == 2) {
				writer.write(parts[0] + "\t" + parts[1]); // Écrire "ClusterID \t Coordinates"
				writer.newLine();
			}
		}

		reader.close();
		writer.close();
	}

}






