import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class equijoin {

    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> {

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            outputCollector.collect(new Text(value.toString().split(",")[1]),value);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

        public static String relation_1 = "";
        public static String relation_2 = "";

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

            ArrayList<String> relation_1_values = new ArrayList<String>();
            ArrayList<String> relation_2_values = new ArrayList<String>();


            while(values.hasNext()){
                Text row = values.next();
                String[] fieldValues = row.toString().split(",");

                if(fieldValues.length != 0) {

                    if(relation_1.equals("")) relation_1 = fieldValues[0];
                    else if(relation_2.equals("") && !relation_1.equals(fieldValues[0])) relation_2 = fieldValues[0];

                    if (fieldValues[0].equals(relation_1)) relation_1_values.add(row.toString());
                    else relation_2_values.add(row.toString());
                }
            }

            for(int i=0;i<relation_1_values.size();i++){
                for(int j=0;j<relation_2_values.size();j++){
                    outputCollector.collect(new Text(""),new Text(relation_1_values.get(i)
                            + ", " + relation_2_values.get(j)));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{

        Path inputFilePath = new Path(args[0]);
        Path outputFilePath = new Path(args[1]);

        JobConf job = new JobConf(equijoin.class);
        job.setJobName("equijoin");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.set("mapred.textoutputformat.separator"," ");

        FileInputFormat.setInputPaths(job,inputFilePath);
        FileOutputFormat.setOutputPath(job,outputFilePath);

        JobClient.runJob(job);

    }
}
