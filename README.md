![Consumer Insights](src/angular/src/assets/cits.png)

## Motivation
“Not being customer-centric is the biggest threat to any business”

For example, I produce Bose Speakers. As a product maker, I would be interested to know what people are talking about
my product.
* What do they like?
* What're they annoyed about?
* What do they expect?
* Did their expectations change over the time?

## Solution
Link to Consumer Insights: [consumerinsights.info:4200](http://consumerinsights.info:4200/)

A search platform for product makers where they conduct their market research, understand what people are
talking about various products in the market


## Data
[Amazon Customer Reviews Dataset](https://registry.opendata.aws/amazon-reviews/)
* 17 Years (1999-2015)
* 50 GB
* 9M+ Products
* 20M+ Users
* 130M+ Reviews


## Pipeline
![Consumer Insights](src/angular/src/assets/pipeline.png)
* Initially reviews are stored in an S3 Bucket 
* Spark loads a new batch of reviews after every one hour into memory
* Spark groups reviews by product, computes product analytics such as Product Rating, Total Number of Reviews, Reviews Breakdown 
and stores them in Elasticsearch
* After the arrival of every new batch, the spark job performs insert/update on the 
product depending on its existence in the database 
* Airflow schedules spark jobs to process an year of reviews every 1 hour
* Flask and Angular are used to develop the web application


## Presentation link
Link to the presentation: [Consumer Insights](https://docs.google.com/presentation/d/1Y87Zx1paC_1mu5wxPh92PQM2EvLrC9RXcGUGLQR9pNU/edit?usp=sharing)