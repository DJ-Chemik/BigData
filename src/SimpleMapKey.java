import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class SimpleMapKey {

    private String street;
    private int zipcode;
    private String typeParticipant;
    private String charackerOfAccident;

    public SimpleMapKey() {
    }

    @Override
    public String toString() {
        String separator = ", ";
        return zipcode + separator + street +
                separator + typeParticipant +
                separator + charackerOfAccident;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public int getZipcode() {
        return zipcode;
    }

    public void setZipcode(int zipcode) {
        this.zipcode = zipcode;
    }

    public String getTypeParticipant() {
        return typeParticipant;
    }

    public void setTypeParticipant(String typeParticipant) {
        this.typeParticipant = typeParticipant;
    }

    public String getCharackerOfAccident() {
        return charackerOfAccident;
    }

    public void setCharackerOfAccident(String charackerOfAccident) {
        this.charackerOfAccident = charackerOfAccident;
    }
}
