// RelationshipMapper.java
package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RelationshipMapper extends Mapper<LongWritable, Relationship, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Relationship value, Context context) 
            throws IOException, InterruptedException {
        // Pour A<->B, émettre A->B et B->A
        outputKey.set(value.getId1());
        outputValue.set(value.getId2());
        context.write(outputKey, outputValue);
        
        outputKey.set(value.getId2());
        outputValue.set(value.getId1());
        context.write(outputKey, outputValue);
    }
}