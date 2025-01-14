package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserRecommendation implements WritableComparable<UserRecommendation> {
    private String user;                // Utilisateur à qui on fait la recommandation
    private String recommendedUser;     // Utilisateur recommandé
    private int commonConnections;      // Nombre de connexions en commun
    private boolean areDirectFriends;   // Sont-ils déjà amis ?

    public UserRecommendation() {
    }

    public UserRecommendation(String user, String recommendedUser, int commonConnections, boolean areDirectFriends) {
        this.user = user;
        this.recommendedUser = recommendedUser;
        this.commonConnections = commonConnections;
        this.areDirectFriends = areDirectFriends;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(user);
        out.writeUTF(recommendedUser);
        out.writeInt(commonConnections);
        out.writeBoolean(areDirectFriends);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        user = in.readUTF();
        recommendedUser = in.readUTF();
        commonConnections = in.readInt();
        areDirectFriends = in.readBoolean();
    }

    @Override
    public int compareTo(UserRecommendation o) {
        // D'abord comparer par utilisateur
        int compare = user.compareTo(o.user);
        if (compare != 0) return compare;

        // Si même utilisateur, trier par connexions communes (décroissant)
        compare = -Integer.compare(commonConnections, o.commonConnections);
        if (compare != 0) return compare;

        // En dernier, trier par nom de l'utilisateur recommandé
        return recommendedUser.compareTo(o.recommendedUser);
    }

    // Getters
    public String getUser() { return user; }
    public String getRecommendedUser() { return recommendedUser; }
    public int getCommonConnections() { return commonConnections; }
    public boolean areDirectFriends() { return areDirectFriends; }

    @Override
    public String toString() {
        return String.format("%s (%d connexions communes%s)", 
                recommendedUser, commonConnections, 
                areDirectFriends ? ", déjà amis" : "");
    }
}