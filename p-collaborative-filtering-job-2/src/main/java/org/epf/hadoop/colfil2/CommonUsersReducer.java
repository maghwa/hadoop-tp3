package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class CommonUsersReducer 
        extends Reducer<UserPair, IntWritable, Text, Text> {
    
    private Text outputKey = new Text();
    private Text result = new Text();

    @Override
    protected void reduce(UserPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        
        int sum = 0;
        boolean areDirectFriends = false;

        // Parcourir toutes les valeurs
        for (IntWritable val : values) {
            if (val.get() == -1) {
                areDirectFriends = true;
            } else {
                sum += val.get();
            }
        }

        // N'émettre que si il y a des relations en commun ou si ce sont des amis directs
        if (sum > 0 || areDirectFriends) {
            outputKey.set(key.toString());
            // Format de sortie : "nombreRelationsCommunes,0" si amis directs
            // ou "nombreRelationsCommunes" sinon
            String resultStr = sum + (areDirectFriends ? ",0" : "");
            result.set(resultStr);
            context.write(outputKey, result);
        }
    }
}