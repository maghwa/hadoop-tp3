package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CommonUsersMapper 
        extends Mapper<LongWritable, Text, UserPair, IntWritable> {

    private final IntWritable ONE = new IntWritable(1);
    private final IntWritable MINUS_ONE = new IntWritable(-1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        // Format d'entrée : utilisateur   relation1,relation2,relation3,...
        String[] parts = value.toString().split("\\s+");
        if (parts.length != 2) return;

        String user = parts[0];
        List<String> relations = Arrays.asList(parts[1].split(","));

        // Pour chaque relation existante, émettre un marqueur négatif
        for (String relation : relations) {
            context.write(new UserPair(user, relation), MINUS_ONE);
        }

        // Pour chaque paire de relations de l'utilisateur
        for (int i = 0; i < relations.size(); i++) {
            String rel1 = relations.get(i);
            for (int j = i + 1; j < relations.size(); j++) {
                String rel2 = relations.get(j);
                // Ces deux relations ont l'utilisateur courant en commun
                context.write(new UserPair(rel1, rel2), ONE);
            }
        }
    }
}