import os, time
import datetime
import datetime as dt



LOCAL_PATH = './realestate_scraping/data/'

#def get_pages_from_local(LOCAL_PATH):
files_list = []
today = dt.datetime.now().date()
print(today)
  
for root, directories, files in os.walk(LOCAL_PATH):
    for _file in files:
        filetime = dt.datetime.fromtimestamp(os.path.getctime(os.path.join(root, _file)))

        if filetime.date() == today:
            files_list.append(os.path.join(root, _file))
   #return files_list
print(len(files_list))