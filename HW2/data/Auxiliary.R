#######################################################
#  Multi-Source Social Feedback of Online News Feeds  #
#######################################################

### LOADING PACKAGES

library(ggplot2)
library(scales)
library(dplyr)
library(grid)

### SET WORKING DIRECTORY

setwd(".")

#######################################################

### LOADING NEWS DATA

news <- read.csv("News_Final.csv") # DATA ON NEWS ITEMS

### LOADING SOCIAL FEEDBACK DATA

## Social Feedback Data - Facebook
facebook_economy <- read.csv("Facebook_Economy.csv")
facebook_microsoft <- read.csv("Facebook_Microsoft.csv") 
facebook_obama <- read.csv("Facebook_Obama.csv") 
facebook_palestine <- read.csv("Facebook_Palestine.csv") 

## Social Feedback Data - Google+
googleplus_economy <- read.csv("GooglePlus_Economy.csv")
googleplus_microsoft <- read.csv("GooglePlus_Microsoft.csv") 
googleplus_obama <- read.csv("GooglePlus_Obama.csv") 
googleplus_palestine <- read.csv("GooglePlus_Palestine.csv") 

## Social Feedback Data - LinkedIn
linkedin_economy <- read.csv("LinkedIn_Economy.csv")
linkedin_microsoft <- read.csv("LinkedIn_Microsoft.csv") 
linkedin_obama <- read.csv("LinkedIn_Obama.csv") 
linkedin_palestine <- read.csv("LinkedIn_Palestine.csv") 

#######################################################

### TEMPORAL PATTERNS

## Average Popularity by Publication Hour

news["Hour"] <- substr(news$PublishDate,12,13)
news$Hour <- as.numeric(news$Hour)
news_facebook <- news[news$Facebook>0,]
news_googleplus <- news[news$GooglePlus>0,]
news_linkedin <- news[news$LinkedIn>0,]
news$Hour <- NULL

facebook.plot <- ggplot(news_facebook, aes(x=Hour, y=log(Facebook), group=Hour)) + geom_boxplot() + ggtitle("Facebook") + ylab("Shares (log)") + scale_y_continuous(limits=c(0,10))
googleplus.plot <- ggplot(news_googleplus, aes(x=Hour, y=log(GooglePlus), group=Hour)) + geom_boxplot() + ggtitle("GooglePlus") + ylab("+1 (log)") + scale_y_continuous(limits=c(0,10))
linkedin.plot <- ggplot(news_linkedin, aes(x=Hour, y=log(LinkedIn), group=Hour)) + geom_boxplot() + ggtitle("LinkedIn") + ylab("Shares (log)") + scale_y_continuous(limits=c(0,10))

rm(news_facebook); rm(news_googleplus); rm(news_linkedin)

## Average Popularity by Publication Weekday

news["Weekday"] <- weekdays(as.Date(news$PublishDate))
news$Weekday <- factor(news$Weekday)
levels(news$Weekday) <- c("Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday")
news_facebook <- news[news$Facebook>0,]
news_googleplus <- news[news$GooglePlus>0,]
news_linkedin <- news[news$LinkedIn>0,]
news$Weekday <- NULL

facebook.plot <- ggplot(news_facebook, aes(x=Weekday, y=log(Facebook), group=Weekday)) + geom_boxplot() + ggtitle("Facebook") + ylab("Shares (log)") + scale_y_continuous(limits=c(0,10)) + theme(axis.text.x = element_text(angle = 45, hjust = 1))
googleplus.plot <- ggplot(news_googleplus, aes(x=Weekday, y=log(GooglePlus), group=Weekday)) + geom_boxplot() + ggtitle("GooglePlus") + ylab("+1 (log)") + scale_y_continuous(limits=c(0,10)) + theme(axis.text.x = element_text(angle = 45, hjust = 1))
linkedin.plot <- ggplot(news_linkedin, aes(x=Weekday, y=log(LinkedIn), group=Weekday)) + geom_boxplot() + ggtitle("LinkedIn") + ylab("Shares (log)") +  scale_y_continuous(limits=c(0,10)) + theme(axis.text.x = element_text(angle = 45, hjust = 1))

rm(news_facebook); rm(news_googleplus); rm(news_linkedin)

#######################################################

### NEWS PER TOPIC/DAY

news_topic <- news[,c("PublishDate","Topic")]
news_topic$Day <- substr(news_topic$PublishDate,1,10)

d <- tbl_df(news_topic[,3:2])

## Graph with number of news per topic
nrNewsTopic <- d %>% group_by(Topic) %>% filter(Day > "2015-11-09")  %>% filter(Day < "2016-07-08")  %>% summarize(nrNews=n())
plot.nrNews_topic <- ggplot(nrNewsTopic,aes(x=Topic,y=nrNews)) + geom_bar(stat="identity") + scale_y_continuous(limits=c(0,40000))

## Graph with number of news per topic per day (2 alternatives: lines or smoothed lines)
nrNewsTopicDay <- d %>% group_by(Topic,Day) %>% filter(Day > "2015-11-09")  %>% filter(Day < "2016-07-08") %>% summarize(nrNews=n()) %>% arrange(Day)
nrNewsTopicDay$Day <- as.Date(nrNewsTopicDay$Day)

plot.nrNews_daily <- ggplot(nrNewsTopicDay,aes(x=Day,y=nrNews,linetype=Topic,group=Topic,color=Topic)) + geom_smooth() +  scale_x_date(labels=date_format("%m-%Y"),breaks=date_breaks("month")) + theme(axis.text.x = element_text(angle = 45, hjust = 1))

rm(news_topic); rm(d); rm(nrNewsTopicDay)

#######################################################

### Create new dataset with bag-of-words (Example for topic 'economy') when applied to Headline of news items

library(tm)
library(qdap)

news.economy <- news[news$Topic == "economy",]
news.economy <- news.economy[order(as.POSIXlt(news.economy$PublishDate, "%Y-%m-%d %H:%M:%S"), decreasing = FALSE),]
rownames(news.economy) <- 1:nrow(news.economy)

corpus <- Corpus(VectorSource(news.economy$Headline))
corpus <- tm_map(corpus, content_transformer(tolower))
removeHandles <- function(x) gsub("@[[:alnum:]]*", "", x)
corpus <- tm_map(corpus, content_transformer(removeHandles))
removeURL <- function(x) gsub("http[[:graph:]]+", "", x)
corpus <- tm_map(corpus, content_transformer(removeURL))
removeStrange <- function(x) gsub("(<.+>)+", "", x)
corpus <- tm_map(corpus, content_transformer(removeStrange))
myStopwords <- c(stopwords('english'),"economy","next","break","else","terms","while")
corpus <- tm_map(corpus, content_transformer(removeWords), myStopwords)
corpus <- tm_map(corpus, content_transformer(removePunctuation))
corpus <- tm_map(corpus, content_transformer(removeNumbers))
corpus <- tm_map(corpus, content_transformer(stripWhitespace))
DTM_train <- DocumentTermMatrix(corpus, control=list(wordLengths=c(4,Inf)))
DTM_train <- removeSparseTerms(DTM_train,0.99)
rowTotals <- apply(DTM_train, 1, sum)

matrix_train <- as.matrix(DTM_train)
frame_train <- as.data.frame(matrix_train)
frame_train <- frame_train[rowTotals>0,]

frame_train["IDLink"] <- NA
frame_train$IDLink <- as.numeric(news.economy[rownames(frame_train),]$IDLink)

frame_train["PublishDate"] <- NA
frame_train$PublishDate <- as.POSIXct(news.economy[rownames(frame_train),]$PublishDate)

frame_train["SentimentTitle"] <- NA
frame_train$SentimentTitle <- as.numeric(news.economy[rownames(frame_train),]$SentimentTitle)

frame_train["SentimentHeadline"] <- NA
frame_train$SentimentHeadline <- as.numeric(news.economy[rownames(frame_train),]$SentimentHeadline)

frame_train