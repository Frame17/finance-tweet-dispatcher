# Finance Tweet Dispatcher

This project is a financial tweet dispatcher built with Kotlin and Spark using Kafka.
The dispatcher streams the tweets from the general Kafka topic, 
classifies and routes them to specific Kafka topics based on the classification result.
A simple classifier is trained beforehand on a financial tweet dataset.

## Dataset

The financial tweet dataset contains tweets related to finance and stock market, which are labeled with one of 20
categories:

| Label | Name                        |
|-------|-----------------------------|
| 0     | Analyst Update              |
| 1     | Fed / Central Banks         |
| 2     | Company / Product News      |
| 3     | Treasuries / Corporate Debt |
| 4     | Dividend                    |
| 5     | Earnings                    |
| 6     | Energy / Oil                |
| 7     | Financials                  |
| 8     | Currencies                  |
| 9     | General News / Opinion      |
| 10    | Gold / Metals / Materials   |
| 11    | IPO                         |
| 12    | Legal / Regulation          |
| 13    | M&A / Investments           |
| 14    | Macro                       |
| 15    | Markets                     |
| 16    | Politics                    |
| 17    | Personnel Change            |
| 18    | Stock Commentary            |
| 19    | Stock Movement              |


## Acknowledgements

This project uses the financial tweet dataset from https://www.kaggle.com/datasets/sulphatet/twitter-financial-news.