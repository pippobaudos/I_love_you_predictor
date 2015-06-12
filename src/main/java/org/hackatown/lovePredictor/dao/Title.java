package org.hackatown.lovePredictor.dao;

import java.io.Serializable;

/**
* Created by niccolo on 6/9/15.
*/ //movie_titles_metadata.txt
// m0 +++$+++ 10 things i hate about you +++$+++ 1999 +++$+++ 6.90 +++$+++ 62847 +++$+++ ['comedy', 'romance']
public class Title implements Serializable {
    String movieId;
    String title;
    Integer year;
    Float rate;
    String[] tags;
}
