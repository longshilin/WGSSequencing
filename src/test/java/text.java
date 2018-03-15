import org.apache.hadoop.io.Text;

public class text {
    public static void main(String[] args){

        Text text = new Text();
        text.set("123");
        text.set("456");
        text.set("12");
        text.set("123");

        String txt = text.toString();
        System.out.println(txt);
    }
}
