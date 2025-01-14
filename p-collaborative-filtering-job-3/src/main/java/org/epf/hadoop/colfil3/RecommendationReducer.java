package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class RecommendationReducer 
        extends Reducer<Text, UserRecommendation, Text, Text> {
    
    private static final int TOP_N = 5;  // Nombre de recommandations à garder
    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<UserRecommendation> values, Context context) 
            throws IOException, InterruptedException {
        
        // PriorityQueue pour garder les TOP_N recommandations triées
        PriorityQueue<UserRecommendation> topRecommendations = 
            new PriorityQueue<>(TOP_N, Collections.reverseOrder());

        // Collecter toutes les recommandations
        for (UserRecommendation val : values) {
            UserRecommendation copy = new UserRecommendation(
                val.getUserId(), 
                val.getRecommendedId(), 
                val.getCommonFriends()
            );
            topRecommendations.add(copy);
        }

        // Construire la liste des meilleures recommandations
        List<String> recommendations = new ArrayList<>();
        while (!topRecommendations.isEmpty() && recommendations.size() < TOP_N) {
            recommendations.add(topRecommendations.poll().toString());
        }

        if (!recommendations.isEmpty()) {
            result.set(String.join(", ", recommendations));
            context.write(key, result);
        }
    }
}