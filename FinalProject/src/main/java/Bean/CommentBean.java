package Bean;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommentBean implements Writable {
    private long user_id;
    private long recipe_id;
    private String date;
    private int rating;
    private String review;

    public CommentBean() {}

    public CommentBean(long user_id, long recipe_id, String date, int rating, String review) {
        this.user_id = user_id;
        this.recipe_id = recipe_id;
        this.date = date;
        this.rating = rating;
        this.review = review;
    }

    public long getUser_id() {
        return user_id;
    }

    public void setUser_id(long user_id) {
        this.user_id = user_id;
    }

    public long getRecipe_id() {
        return recipe_id;
    }

    public void setRecipe_id(long recipe_id) {
        this.recipe_id = recipe_id;
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

    public String getReview() {
        return review;
    }

    public void setReview(String review) {
        this.review = review;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(user_id);
        dataOutput.writeLong(recipe_id);
        dataOutput.writeUTF(date);
        dataOutput.writeInt(rating);
        dataOutput.writeUTF(review);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
            user_id = dataInput.readLong();
            recipe_id = dataInput.readLong();
            date = dataInput.readUTF();
            rating = dataInput.readInt();
            review = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return user_id + "###" + recipe_id + "###" + date + "###" + rating + "###";
    }
}
