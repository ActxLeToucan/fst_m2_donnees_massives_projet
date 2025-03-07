package writable;

import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;

public class PicWritable implements WritableComparable {
	private long instant = 0;
    private double usage = 0;
    private int duration = 0;

    public PicWritable() {
    }

	public PicWritable(long instant, double usage, int duration) {
        this.instant = instant;
        this.usage = usage;
        this.duration = duration;
	}

	public PicWritable(long instant, Text value) {
        this.instant = instant;
        String[] parts = value.toString().split(";");
        this.usage = Double.parseDouble(parts[0]);
        this.duration = Integer.parseInt(parts[1]);
    }


	public long getInstant() {
        return this.instant;
    }

    public double getUsage() {
        return this.usage;
    }

    public int getDuration() {
        return this.duration;
    }

	@Override 
	public void readFields(DataInput in) throws IOException {
		this.instant = in.readLong();
        Text value = new Text();
        value.readFields(in);
        String[] parts = value.toString().split(";");
        this.usage = Double.parseDouble(parts[0]);
        this.duration = Integer.parseInt(parts[1]);
	}

	@Override 
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.instant);
        Text value = new Text(this.usage + ";" + this.duration);
        value.write(out);
	}

	@Override
	public String toString() {
		return this.instant + " " + this.usage + " " + this.duration;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof PicWritable) {
			PicWritable other = (PicWritable) o;
			return this.instant == other.getInstant() && this.usage == other.getUsage() && this.duration == other.getDuration();
		}
		return false;
	}

	@Override
	public int hashCode() {
		return (int) this.instant * (int) this.usage * this.duration;
	}

	@Override
	public int compareTo(Object o){
		if (!(o instanceof PicWritable)) {
			return 1;
		}

		PicWritable other = (PicWritable) o ;
		int cmp = Double.compare(other.getUsage(), this.usage);
        if (cmp != 0) {
            return cmp;
        }
        cmp = Long.compare(this.instant, other.getInstant());
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(other.getDuration(), this.duration);
	}
}
