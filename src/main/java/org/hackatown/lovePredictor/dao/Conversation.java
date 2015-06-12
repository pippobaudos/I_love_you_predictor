package org.hackatown.lovePredictor.dao;

import java.io.Serializable;

/**
* Created by niccolo on 6/7/15.
*/ //movie_conversations.txt
// u0 +++$+++ u2 +++$+++ m0 +++$+++ ['L194', 'L195', 'L196', 'L197']
public class Conversation implements Serializable {


    String userId1;
    String userId2;
    String movieId;

    String gender1;
    String gender2;


    int idLine;
    String line;
    String content;

    private boolean love = false;
    private String firstILoveYouConversation;

    public Conversation() {
    }

    public String getUserId1() {
        return userId1;
    }

    public String getUserId2() {
        return userId2;
    }

    public String getMovieId() {
        return movieId;
    }

    public void setUserId1(String userId1) {
        this.userId1 = userId1;
    }

    public void setUserId2(String userId2) {
        this.userId2 = userId2;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public String getLine() {
        return line;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getGender1() {
        return gender1;
    }

    public String getGender2() {
        return gender2;
    }

    public String getGenders() {
        if (gender2.equals("?")) {
            return gender1 + "?";
        }
        if (gender1.equals("?")) {
            return gender2 + "?";
        }
        if (gender1.compareTo(gender2) < 0) {
            return gender1 + gender2;
        }
        else{
            return gender2 + gender1;
        }
    }

    public void setGender1(String gender1) {
        this.gender1 = gender1;
    }

    public void setGender2(String gender2) {
        this.gender2 = gender2;
    }

    public int getIdLine() {
        return idLine;
    }

    public void setIdLine(int idLine) {
        this.idLine = idLine;
    }

    @Override
    public String toString() {
        return "Conversation{" +
                "userId1='" + userId1 + '\'' +
                ", userId2='" + userId2 + '\'' +
                ", movieId='" + movieId + '\'' +
                ", line='" + line + '\'' +
                ", gender1='" + gender1 + '\'' +
                ", gender2='" + gender2 + '\'' +
                ", content=" + content +
                '}';
    }

    public String getKeyMoviedUsers() {
        StringBuilder sb = new StringBuilder();
        sb.append(movieId);
        sb.append("-");
        if (userId1.compareTo(userId2) < 0) {
            sb.append(userId1);
            sb.append("-");
            sb.append(userId2);
        }
        else {
            sb.append(userId2);
            sb.append("-");
            sb.append(userId1);
        }
        return sb.toString();
    }

    public void setLove(boolean love) {
        this.love = love;
    }

    public boolean getLove() {
        return love;
    }

    public void setFirstILoveYouConversation(String firstILoveYouConversation) {
        this.firstILoveYouConversation = firstILoveYouConversation;
    }

    public String getFirstILoveYouConversation() {
        return this.firstILoveYouConversation==null?"":this.firstILoveYouConversation;
    }

    public void addContentAndLine(Conversation toAdd, String sep) {
        this.content += sep + toAdd.content;
        this.line += sep + toAdd.line;
    }
}
