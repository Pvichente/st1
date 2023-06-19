import os
import urllib.parse
import pandas as pd 
import requests 
from bs4 import BeautifulSoup
import time
from dotenv import load_dotenv

# env variables
ENV_INPUT_PATH = "INPUT_PATH"
ENV_OUTPUT_PATH = "OUTPUT_PATH"
ENV_MAX_STORIES = "MAX_STORIES"
ENV_SEARCH_WORDS = "SEARCH_WORDS"
ENV_ADD_SEARCH_BY_COMPANY_NAME = "ADD_SEARCH_BY_COMPANY_NAME"

# load variables
load_dotenv()
PATH = os.path.dirname(__file__)

DATAFILE = os.path.join(PATH, os.getenv(ENV_INPUT_PATH))
OUTPUT_FILE = os.path.join(PATH, os.getenv(ENV_OUTPUT_PATH))
MAX_STORIES = int(os.getenv(ENV_MAX_STORIES))

SEARCH_WORDS = os.getenv(ENV_SEARCH_WORDS).split(",")
if os.getenv(ENV_ADD_SEARCH_BY_COMPANY_NAME):
  SEARCH_WORDS.append("")

# constants
# columns
PRIMARY_KEY = "Clave"
COMPANY_FULL_NAME = "Razón Social"
COMPANY_SHORT_NAME = "Nombre Corto"

TITLE = "Titulo"
URL = "url"
DATE = "Fecha"

OUTPUT_COLUMNS = [PRIMARY_KEY, TITLE, DATE, URL]

# URLs
GOOGLE_URL = "https://news.google.com"
BASE_URL = "https://news.google.com/search?q="
URL_TRAILER = "&hl=es-419&gl=MX&ceid=MX:es-419"

# miscellaneous
TAIL_INDICATORS = { "s.a.", "s.c." }
ARTICLE_CLASS = "MQsxIb"
ANCHOR_CLASS = "DY5T1d"
DATE_CLASS = "WW6dff"


def __main__():
  companies = pd.read_excel(DATAFILE)
  companies = sanitize_data(companies)
  news = get_news(companies)
  news.to_csv(OUTPUT_FILE, index=False)

def sanitize_data(data: pd.DataFrame) -> pd.DataFrame:
    data[COMPANY_SHORT_NAME].fillna(0, inplace=True)
    return data.where(data[PRIMARY_KEY].notnull())

def get_news(data: pd.DataFrame) -> pd.DataFrame:
  all_queries = get_possible_search_values(data)
  news = {}
  for key, queries in all_queries.items():
    news[key] = get_news_for_company(queries)
    
  rows = convert_news_to_rows(news)
  return pd.DataFrame.from_dict(rows)
    
def get_possible_search_values(data: pd.DataFrame) -> dict:
  searchable_company_names = {}
  for i in range(len(data)):
    key = data.loc[i, PRIMARY_KEY]
    full_name = data.loc[i, COMPANY_FULL_NAME]
    short_name = data.loc[i, COMPANY_SHORT_NAME]
    
    full_name = full_name if type(full_name) == str else ""
    short_name = short_name if type(short_name) == str else ""
    if not (full_name or short_name):
      continue
    possible_search_values = get_queries_for_company(full_name, short_name)
    searchable_company_names[key] = possible_search_values
  return searchable_company_names

def get_queries_for_company(full_name: str, short_name: str = "") -> tuple:
  company_names = get_company_names(full_name, short_name)
  return (f'"{search_word}"+"{name}"' for search_word in SEARCH_WORDS for name in company_names)

def get_company_names(full_name: str, short_name: str = "") -> tuple:
  full_name = full_name if type(full_name) == str else short_name
  all_parts = full_name.split(",")
  index = 0
  for i in range(len(all_parts)):
    for indicator in TAIL_INDICATORS:
      if indicator in all_parts[i].lower():
        index = i - 1 # not to include everything after
        break
  result = all_parts[:index + 1]
  if not full_name in all_parts[:index + 1]:
    result = [full_name,] + result
  if 0 < len(short_name):
    result.append(short_name)
  return tuple(result)

def get_news_for_company(queries: tuple) -> list:
  news = []
  for query in queries:
    if MAX_STORIES < len(news):
      break
    news += get_news_for_query(query)
  return news

def get_news_for_query(query: str) -> list:
  result = []
  url = BASE_URL + urllib.parse.quote(query) + URL_TRAILER
  # time.sleep(100/1000) # wait 100ms to have at most 10 requests per second
  response = requests.get(url)
  soup = BeautifulSoup(response.text, "html.parser")
  
  stories = soup.find_all("article", class_=ARTICLE_CLASS)
  stories = stories[:min(len(stories), MAX_STORIES)]
  for story in stories:
    news = {}
    news[TITLE] = story.find("a", class_=ANCHOR_CLASS).text
    news[URL] = GOOGLE_URL + story.find("a", class_=ANCHOR_CLASS).get("href")[1:]
    news[DATE] = story.find("time", class_=DATE_CLASS).text
    result.append(news)
  return result

def convert_news_to_rows(news: dict) -> dict:
  keys = []
  titles = []
  dates = []
  urls = []
  for key, news in news.items():
    for article in news[:min(len(news), MAX_STORIES)]:
      keys.append(key)
      titles.append(article.get(TITLE, ""))
      dates.append(article.get(DATE, ""))
      urls.append(article.get(URL, ""))
    
  return {
    PRIMARY_KEY: keys,
    TITLE: titles,
    DATE: dates,
    URL: urls
  }

__main__()