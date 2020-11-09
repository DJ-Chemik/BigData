import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapKey implements WritableComparable<MapKey> {

    private Text street;
    private IntWritable zipcode;
    private Text typeParticipant;
    private Text charackerOfAccident;

    public MapKey() {
    }

    public Text getStreet() {
        return street;
    }

    public void setStreet(Text street) {
        this.street = street;
    }

    public IntWritable getZipcode() {
        return zipcode;
    }

    public Text getTypeParticipant() {
        return typeParticipant;
    }

    public void setTypeParticipant(Text typeParticipant) {
        this.typeParticipant = typeParticipant;
    }

    public Text getCharackerOfAccident() {
        return charackerOfAccident;
    }

    public void setCharackerOfAccident(Text charackerOfAccident) {
        this.charackerOfAccident = charackerOfAccident;
    }

    public void setZipcode(IntWritable zipcode) {
        this.zipcode = zipcode;
    }

    @Override
    public int compareTo(MapKey o) {
        return 0;
    }

    @Override
    public String toString() {
        String separator = ", ";
        return zipcode.toString() +
                separator + street.toString() +
                separator + typeParticipant.toString() +
                separator + charackerOfAccident.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        zipcode.write(dataOutput);
        street.write(dataOutput);
        typeParticipant.write(dataOutput);
        charackerOfAccident.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        zipcode.readFields(dataInput);
        street.readFields(dataInput);
        typeParticipant.readFields(dataInput);
        charackerOfAccident.readFields(dataInput);
    }
}
