package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RecommendationMapper extends Mapper<LongWritable, Text, Text, UserRecommendation> {
    private Text outputKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        // Format d'entrée : user1,user2    nombreDeRelationsCommunes
        String[] parts = value.toString().split("\t");
        if (parts.length != 2) return;

        // Séparer les utilisateurs
        String[] users = parts[0].split(",");
        if (users.length != 2) return;

        // Lire le nombre de relations communes
        int commonFriends = Integer.parseInt(parts[1]);

        // Émettre une recommandation dans les deux sens
        // user1 -> user2
        outputKey.set(users[0]);
        context.write(outputKey, new UserRecommendation(
            users[0], users[1], commonFriends));

        // user2 -> user1
        outputKey.set(users[1]);
        context.write(outputKey, new UserRecommendation(
            users[1], users[0], commonFriends));
    }
}