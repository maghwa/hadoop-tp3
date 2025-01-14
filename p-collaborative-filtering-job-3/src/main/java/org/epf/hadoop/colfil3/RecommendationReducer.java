package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class RecommendationReducer extends Reducer<Text, UserRecommendation, Text, Text> {
    private static final int TOP_N = 5;
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<UserRecommendation> values, Context context) 
            throws IOException, InterruptedException {
        
        // Utiliser un TreeSet pour garder les recommandations triées
        TreeSet<UserRecommendation> topRecommendations = new TreeSet<>((a, b) -> {
            // D'abord trier par nombre de connexions (décroissant)
            int compare = -Integer.compare(a.getCommonConnections(), b.getCommonConnections());
            if (compare != 0) return compare;
            
            // En cas d'égalité, trier par nom
            return a.getRecommendedUser().compareTo(b.getRecommendedUser());
        });

        // Collecter toutes les recommandations
        for (UserRecommendation val : values) {
            // Ne pas inclure les amis directs dans les recommandations
            if (!val.areDirectFriends()) {
                topRecommendations.add(new UserRecommendation(
                    val.getUser(),
                    val.getRecommendedUser(),
                    val.getCommonConnections(),
                    val.areDirectFriends()
                ));
            }
            
            // Ne garder que les TOP_N meilleures
            if (topRecommendations.size() > TOP_N) {
                topRecommendations.pollLast();
            }
        }

        // Formater le résultat
        if (!topRecommendations.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (UserRecommendation rec : topRecommendations) {
                if (sb.length() > 0) sb.append(", ");
                sb.append(rec.toString());
            }
            result.set(sb.toString());
            context.write(key, result);
        }
    }
}