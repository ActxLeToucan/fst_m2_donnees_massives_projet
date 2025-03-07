package reducer;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PicDetectionReducer extends Reducer<LongWritable, DoubleWritable, LongWritable, Text> {
	private long startPic = 0;
	private double previousValue = 0;
	private double sumUsage = 0;
	private boolean inPic = false;

	@Override
	public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		if (!values.iterator().hasNext()) {
			return;
		}
		double currentValue = values.iterator().next().get(); // on n'a qu'une seule valeur à un instant donné
		long now = key.get();

		// debut d'un pic
		if (currentValue > previousValue) {
			if (!inPic) {
				startPic = now - 1; // si on monte, c'est que le pic a commencé avant
				inPic = true;
			}

			sumUsage += currentValue;
			previousValue = currentValue;
			return;
		}

		// stagnation
		if (currentValue == previousValue) {
			inPic = false; // on considère que c'est pas un pic
			sumUsage = 0;
			return;
		}

		// fin d'un pic
		if (inPic) {
			long duration = now - startPic;
			context.write(new LongWritable(startPic), new Text(sumUsage + ";" + duration));
			inPic = false;
			sumUsage = 0;
		}
	}
}
