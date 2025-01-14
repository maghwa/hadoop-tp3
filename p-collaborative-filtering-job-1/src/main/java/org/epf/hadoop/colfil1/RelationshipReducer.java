// RelationshipReducer.java
package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.TreeSet;

public class RelationshipReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        // Utiliser TreeSet pour trier et dédupliquer automatiquement
        TreeSet<String> relations = new TreeSet<>();
        
        // Collecter toutes les relations
        for (Text val : values) {
            relations.add(val.toString());
        }
        
        // Joindre avec des virgules
        result.set(String.join(",", relations));
        context.write(key, result);
    }
}