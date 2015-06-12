package org.hackatown.lovePredictor.dao;

import java.io.Serializable;

/**
 * Created by niccolo on 6/9/15.
 */
    //movie_lines.txt
    // L1045 +++$+++ u0 +++$+++ m0 +++$+++ BIANCA +++$+++ They do not!
    public class Line implements Serializable {
        String lineId;
        String userId;
        String movieId;
        String name;
        String conversation;

    public String getLineId() {
        return lineId;
    }

    public String getUserId() {
        return userId;
    }

    public String getMovieId() {
        return movieId;
    }

    public String getName() {
        return name;
    }

    public String getConversation() {
        return conversation;
    }

    public void setLineId(String lineId) {
        this.lineId = lineId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setConversation(String conversation) {
        this.conversation = conversation;
    }
}
