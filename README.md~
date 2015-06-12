# I Love You Predictor, 
##Machine learning experiment after a fun hackatown at DataScienceLondon

##### @ds_ldn [#gameshack](http://www.meetup.com/Data-Science-London/events/222708603/)

Started as funny discussion during a recent hackatown, this experiment is about building a classifier able to predict if in the next conversation between a couple, one of then would declare his love to his patner, telling him/her: "I love you" 

Using a movie-conversation dataset composed of 220,000 conversations from movies, we have first selected all the couple that has had a conversation.
Then we have split the couples in two groups, the ones fallen in love and all the others.

The first group is about couple in which one of them has declared "I love you" to the other. For this couple we have selected the previous 5 conversations to the love declaration, as training data. For all the other couples, we have randomly selected a group of 5 following conversation.
Then, build a predictor on Azure that looks to work enough well:

![Model Performance](https://dl.dropboxusercontent.com/u/7335663/machine_learning_experiment/IsFallingInLove.png "Performance Predictor of IS FALLING IN LOVE")

We have used the top 500 unigrams, from the text content. To train and validate the model we have used the followig dataset

![ExcelScreenshot](https://dl.dropboxusercontent.com/u/7335663/machine_learning_experiment/datasetBuiltToTrain.png "Data set used to train/validate")

You can download here the dataset built, to train/validate the model:

[Download the CSV dataset to train/validate the model](https://dl.dropboxusercontent.com/u/7335663/machine_learning_experiment/I_love_you_predictor.csv)


That's the model built on Azure ML, used the prebuild sentimental analysis for Twitter, setting the top 500 unigrams like features.

![Model on Azure ML](https://dl.dropboxusercontent.com/u/7335663/machine_learning_experiment/AzureModel.png "Model on Azure ML, used to train/validate")


This project contains some Apache Spark code used to build the data-set, starting from the original csv files of the full movie-datasets



