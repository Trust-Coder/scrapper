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
  dictionary = config.dictionary
  DEST = config.DEST

  company = ""
  title = ""
  location = ""
  val = story.lower()

  for match in dictionary:
    if match in val:
	    index = find_index(val, match)
	    index2 = find_index(val, DEST)
	    company = story[0:index]
	    index = index+len(match)
	    title = story[index:index2]
		
	    if(DEST in story):
	        location = story[index2 + 4:]
		
	    insert_record(cursor, id, company, title, location)

	    break

  
def insert_record(cursor, id, company, title, location):
	#mycursor = mydb.cursor()
	sql = "INSERT INTO jobs (id, company, title, location) VALUES (%s, %s, %s, %s)"
	val = (id, company, title, location)
	cursor.execute(sql, val)
	#print(cursor.rowcount, "record inserted.")
	
def record_exists(cursor, id):
    cursor.execute("SELECT title FROM jobs WHERE id = %s", (id,))
    data=cursor.fetchall()
    if len(data)==0:
        return False
    else: 
        return True
	


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="webscrap"
)
baseURL = "https://news.ycombinator.com/"
jobs_url = baseURL + "jobs"
page = urllib.request.urlopen(jobs_url)
soup = BeautifulSoup(page, 'html.parser')

mycursor = mydb.cursor()
shouldRun = True
count = 1

while(shouldRun):
    print("Scrapping page: " + str(count))
    count = count + 1
    page = urllib.request.urlopen(jobs_url)
    soup = BeautifulSoup(page, 'html.parser')
    table = soup.find('table', {"class":"itemlist"})
    rows = table.findAll('tr', {"class":"athing"})
    morelink = soup.find('a', {"class":"morelink"})

    if(morelink is not None):
        jobs_url = baseURL + morelink.get('href')
        print(jobs_url)
        
    #print(table.prettify())
    for row in rows:
        id = row.get('id')
        if(not record_exists(mycursor, id)):
            story = row.find('a', {"class":"storylink"}).getText()
            print("Story: " + story)
            parse_info(mycursor, id, story)
            mydb.commit()
        else:
            shouldRun = False
            break		

#mydb.commit()
	
	
	#print(row.get('id') + story)


			