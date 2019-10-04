import datetime
import requests
from bs4 import BeautifulSoup
import pymongo
import pandas as pd
from newspaper import Article


class NewsScrapper:

    def news_scrapper(self):

        # Create connection to MongoDB.
        client = pymongo.MongoClient("mongodb://localhost:27017/")

        # Initialize database
        database = client["scrap_database"]

        # Get Date and Time of scrapping.
        scrap_date = datetime.datetime.now().strftime("%Y-%m-%d")
        scrap_time = datetime.datetime.now().time().strftime("%H:%M:%S")
        scrap_date_time = scrap_date + scrap_time

        # Read a CSV file to take input such as search keyword and language.
        data_frame = pd.read_csv('keyword.csv')

        # Create a file name with date and time stamp to save data.
        file_name = "StockIndex-" + scrap_date_time

        # df = pd.read_csv('korean_keyword.csv')
        # file_name = "KoreaStockIndex-" + scrap_date_time

        return self.get_article(data_frame, scrap_date_time, database, file_name)

    def get_article(self, data_frame, scrap_date_time, database, file_name):

        scrap_data_list = []
        for index, row in data_frame.iterrows():

            search_query = "https://www.google.com/search?&q=" + row['Keyword'] +\
                           "&tbs=qdr:h24&tbo=1&source=lnms&tbm=nws&cad=h&hl=" + \
                row['Language']

            result = requests.get(search_query)
            soup = BeautifulSoup(result.text, "html.parser")

            search_news_headline = soup.find_all("h3")
            search_news_website = soup.find_all("div", "slp")
            search_news_article = soup.find_all("div", "st")
            search_news_link = soup.find_all('h3', attrs={'class': 'r'})

            for i in range(len(search_news_article)):
                news_headline = search_news_headline[i].text[:-3]
                news_link = search_news_link[i].a['href'][7:].split('&')[
                    0].split('%')[0]

                try:
                    article = Article(news_link, language=row['Language'])
                    article.download()
                    article.parse()
                    full_news = article.text.replace('\n', '')
                except:
                    full_news = search_news_article[i].text

                # Create a dictionary of single news article with all details.
                data_dict = {"Scrap Date & Time": scrap_date_time,
                             "Language": row['Language'],
                             "News Title": news_headline,
                             "Website & Time Published": search_news_website[i].text,
                             "News Link": news_link,
                             "Full News Article": full_news
                             }

                scrap_data_list.append(data_dict)
        return self.save_data(scrap_data_list, database, file_name)

    # Method to save data into MongoDB Database or write to a text file
    def save_data(self, scrap_data_list, database, file_name):

        # Create a New collection and write data in it.
        collection = database[file_name]
        collection.insert_many(scrap_data_list)

        # Uncomment the following three line code to write data into a Text file
        file = open(file_name + ".txt", "w")
        file.write(str(scrap_data_list))
        file.close()

        # Reading and printing the data from collection on terminal
        for y in collection.find():
            print(y)


# Make the object of class NewsScrapper
news_scrapper_obj = NewsScrapper()
# Call the news_scrapper() method using object
news_scrapper_obj.news_scrapper()
