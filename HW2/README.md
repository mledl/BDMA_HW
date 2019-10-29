# Report and ToDo's

The Reports and ToDo's are managed via Google-Drive:
https://docs.google.com/document/d/1yXQs46TsnMawhvKpFTzkLP4XK9WeBaSXZwqd73Dx-QY/edit#

# Dataset

The used dataset is available at UCI Machine Learning Repository:
https://archive.ics.uci.edu/ml/datasets/News+Popularity+in+Multiple+Social+Media+Platforms

# Dataset Description

Multi-Source Social Feedback of Online News Feeds
Nuno Moniz and Luís Torgo	
The data set is made available under a CC-BY license

## REFERENCE

  Nuno Moniz and Luís Torgo (2018), “Multi-Source Social Feedback of Online News Feeds”,
  CoRR, abs/1801.07055
  
  ```json
  @Article{Moniz2018,
    title = {Multi-Source Social Feedback of Online News Feeds},
    author = {Nuno Moniz and Lu\’is Torgo},
    year = {2018},
    ee = {https://arxiv.org/abs/1801.07055},
    volume = {abs/1801.07055},
    journal = {CoRR},
  }
  ```

## VARIABLES OF NEWS DATA

  1. IDLink (numeric): Unique identifier of news items
  2. Title (string): Title of the news item according to the official media sources
  3. Headline (string): Headline of the news item according to the official media sources
  4. Source (string): Original news outlet that published the news item
  5. Topic (string): Query topic used to obtain the items in the official media sources
  6. PublishDate (timestamp): Date and time of the news items' publication
  7. SentimentTitle (numeric): Sentiment score of the text in the news items' title
  8. SentimentHeadline (numeric): Sentiment score of the text in the news items' headline
  9. Facebook (numeric): Final value of the news items' popularity according to the social media source Facebook
  10. GooglePlus (numeric): Final value of the news items' popularity according to the social media source Google+
  11. LinkedIn (numeric): Final value of the news items' popularity according to the social media source LinkedIn

## VARIABLES OF SOCIAL FEEDBACK DATA

  1. IDLink (numeric): Unique identifier of news items
  2. TS1 (numeric): Level of popularity in time slice 1 (0-20 minutes upon publication)
  3. TS2 (numeric): Level of popularity in time slice 2 (20-40 minutes upon publication)
  4. TS... (numeric): Level of popularity in time slice ...
  5. TS144 (numeric): Final level of popularity after 2 days upon publication
