import org.apache.commons.cli.*;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by cgqfzy on 4/4/17.
 */
public class Test {

    public static void main(String[] args) {
        String[] arg = {"-t", "-c", "hello"};
        try {
            testOption(arg);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    static void testOption(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("t", false, "display current time");
        options.addOption("c", true, "country code");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("t")) {
            System.out.println((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date()) + " of " + cmd.getOptionValue("c"));
        } else {
            System.out.println((new SimpleDateFormat("yyyy-MM-dd")).format(new Date()));
        }
    }
}