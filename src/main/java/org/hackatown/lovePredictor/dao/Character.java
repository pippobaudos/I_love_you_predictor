package org.hackatown.lovePredictor.dao;

import java.io.Serializable;

/**
* Created by niccolo on 6/7/15.
*/ //movie_characters_metadata.txt
// u0 +++$+++ BIANCA +++$+++ m0 +++$+++ 10 things i hate about you +++$+++ f +++$+++ 4
public class Character implements Serializable {
    String userId;
    String name;
    String movieId;
    String title;
    String gender;
    Integer age;

    public String getUserId() {
        return userId;
    }

    public String getName() {
        return name;
    }

    public String getMovieId() {
        return movieId;
    }

    public String getTitle() {
        return title;
    }

    public String getGender() {
        return gender;
    }

    public Integer getAge() {
        return age;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}
