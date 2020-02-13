package Bean;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecipeBean implements Writable {
    private long recipe_id;
    private String name;
    private int min;
    private int numOfIngredients;

    public RecipeBean() {}

    public RecipeBean(long recipe_id, String name, int min, int numOfIngredients) {
        this.recipe_id = recipe_id;
        this.name = name;
        this.min = min;
        this.numOfIngredients = numOfIngredients;
    }

    public long getRecipe_id() {
        return recipe_id;
    }

    public void setRecipe_id(long recipe_id) {
        this.recipe_id = recipe_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getMin() {
        return min;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public int getNumOfIngredients() {
        return numOfIngredients;
    }

    public void setNumOfIngredients(int numOfIngredients) {
        this.numOfIngredients = numOfIngredients;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(recipe_id);
        dataOutput.writeUTF(name);
        dataOutput.write(min);
        dataOutput.write(numOfIngredients);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        recipe_id = dataInput.readLong();
        name = dataInput.readUTF();
        min = dataInput.readInt();
        numOfIngredients = dataInput.readInt();
    }
}
