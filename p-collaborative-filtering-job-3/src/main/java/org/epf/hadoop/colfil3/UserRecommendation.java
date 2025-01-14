package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserRecommendation implements WritableComparable<UserRecommendation> {
    private String userId;         // Utilisateur pour qui on fait la recommandation
    private String recommendedId;  // Utilisateur recommandé
    private int commonFriends;    // Nombre d'amis en commun

    // Constructeur par défaut requis par Hadoop
    public UserRecommendation() {
        this.userId = "";
        this.recommendedId = "";
        this.commonFriends = 0;
    }

    // Constructeur avec paramètres
    public UserRecommendation(String userId, String recommendedId, int commonFriends) {
        this.userId = userId;
        this.recommendedId = recommendedId;
        this.commonFriends = commonFriends;
    }

    // Getters
    public String getUserId() { return userId; }
    public String getRecommendedId() { return recommendedId; }
    public int getCommonFriends() { return commonFriends; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userId);
        out.writeUTF(recommendedId);
        out.writeInt(commonFriends);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId = in.readUTF();
        recommendedId = in.readUTF();
        commonFriends = in.readInt();
    }

    @Override
    public int compareTo(UserRecommendation o) {
        // Trier d'abord par nombre d'amis communs (décroissant)
        int compare = -Integer.compare(this.commonFriends, o.commonFriends);
        if (compare != 0) return compare;
        
        // En cas d'égalité, trier par ID de l'utilisateur recommandé
        return this.recommendedId.compareTo(o.recommendedId);
    }

    @Override
    public String toString() {
        return recommendedId + "(" + commonFriends + ")";
    }
}