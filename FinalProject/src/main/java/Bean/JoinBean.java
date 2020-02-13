package Bean;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinBean implements Writable{
    private long recipe_id;
    private String recipeName;
    private long user_id;
    private String date;
    private int rating;
    private String flag;

    public JoinBean() {}

    public JoinBean(long recipe_id, String recipeName,long user_id, String date, int rating, String flag) {
        this.recipe_id = recipe_id;
        this.recipeName = recipeName;
        this.user_id = user_id;
        this.date = date;
        this.rating = rating;
        this.flag = flag;
    }

    public long getRecipe_id() {
        return recipe_id;
    }

    public void setRecipe_id(long recipe_id) {
        this.recipe_id = recipe_id;
    }

    public String getRecipeName() {
        return recipeName;
    }

    public void setRecipeName(String recipeName) {
        this.recipeName = recipeName;
    }

    public long getUser_id() {
        return user_id;
    }

    public void setUser_id(long user_id) {
        this.user_id = user_id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(recipe_id);
        dataOutput.writeUTF(recipeName);
        dataOutput.writeLong(user_id);
        dataOutput.writeUTF(date);
        dataOutput.writeInt(rating);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        recipe_id = dataInput.readLong();
        recipeName = dataInput.readUTF();
        user_id = dataInput.readLong();
        date = dataInput.readUTF();
        rating = dataInput.readInt();
        flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return recipe_id + "," + recipeName + "," + user_id + "," + date + "," + rating + "," + flag + "\n";
    }
}
