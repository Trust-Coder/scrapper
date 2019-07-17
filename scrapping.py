from bs4 import BeautifulSoup
import urllib.request
import mysql.connector
import config


def find_index(s, char):
    index = 0
    c = char[0]

    for ch in s:
        if ch == c:
            if s[index:index+len(char)] == char:
                return index
        index += 1


def parse_info(cursor, id, story):
    company = ""
    title = ""
    location = ""
    val = story.lower()

    for match in config.DICTIONARY:
        if match in val:
            index = find_index(val, match)
            index2 = find_index(val, config.DEST)
            company = story[0:index]
            index = index+len(match)
            title = story[index:index2]

            if(config.DEST in story):
                location = story[index2 + 4:]

            insert_record(cursor, id, company, title, location)
            break


def insert_record(cursor, id, company, title, location):
    sql = "INSERT INTO jobs (id, company, title, location)"\
          "  VALUES (%s, %s, %s, %s)"
    val = (id, company, title, location)
    cursor.execute(sql, val)


def record_exists(cursor, id):
    cursor.execute("SELECT title FROM jobs WHERE id = %s", (id,))
    data = cursor.fetchall()

    if len(data) == 0:
        return False
    else:
        return True


mydb = mysql.connector.connect(
  host=config.HOSTNAME,
  user=config.DB_USERNAME,
  passwd=config.DB_PASSWORD,
  database=config.DATABASE
)

baseURL = "https://news.ycombinator.com/"
jobs_url = baseURL + "jobs"
page = urllib.request.urlopen(jobs_url)
soup = BeautifulSoup(page, 'html.parser')

mycursor = mydb.cursor()
should_run = True
count = 1

while(should_run):
    print("Scrapping page: " + str(count))
    count = count + 1

    page = urllib.request.urlopen(jobs_url)
    soup = BeautifulSoup(page, 'html.parser')
    table = soup.find('table', {"class": "itemlist"})
    rows = table.findAll('tr', {"class": "athing"})
    morelink = soup.find('a', {"class": "morelink"})

    if(morelink is not None):
        jobs_url = baseURL + morelink.get('href')
        print(jobs_url)

    for row in rows:
        id = row.get('id')

        if(not record_exists(mycursor, id)):
            story = row.find('a', {"class": "storylink"}).getText()
            parse_info(mycursor, id, story)
            mydb.commit()
            print("New Story: " + story)
        else:
            should_run = False
            break
