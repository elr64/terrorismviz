import pandas as pd
from scipy import stats
import requests
import json
import time
from datetime import datetime
import urllib
import dateutil.parser as dparser

def getPushshiftData(group, file):
    r = requests.get('https://api.pushshift.io/reddit/comment/search/',
                     params={'q': f'"{group}"', 'size': 1000})
    print(r)
    d = json.loads(r.text)
    
    if len(d['data']) == 1000:
        data=d['data']
        i=d['data'][999]
        before=i['created_utc']
        while True:
            r = requests.get('https://api.pushshift.io/reddit/comment/search/',
                     params={'q': f'"{group}"', 
                             'size': 1000,
                             'before': before})
            d1 = json.loads(r.text)
            data=d1['data']
            if len(data) == 0: break
            print(len(data))
            i=data[-1]
            before=i['created_utc']
            data.append(d1['data'])
            time.sleep(1)
        df1=pd.DataFrame(data)
        df=df1.filter(items=["author", "body", "created_utc", "subreddit"])
        for row in df:
            df['organization']=group
            df['source']="reddit"
        
    else:
        df1=pd.DataFrame(d['data'])
        df=df1.filter(items=["author", "body", "created_utc", "subreddit"])
        for row in df:
            df['organization']=group
            df['source']="reddit"
    with open(file, 'a') as f:
        df.to_csv(f, header=False)
        
def get4chanData(group, keywords, file):
    jsonboards=json.loads(urllib.request.urlopen("https://a.4cdn.org/boards.json").read())
    boardinfo=pd.DataFrame(jsonboards["boards"])
    boards=boardinfo['board']
    titles=boardinfo['title']
    for i,board in enumerate(boards):
        for page in requests.get(f'https://a.4cdn.org/{board}/threads.json').json():
            for page_thread in page['threads']:
                thread_id = page_thread['no']
                r = requests.get(f'https://a.4cdn.org/{board}/thread/{thread_id}.json')
                if r == '<Response [200]>':
                    thread=r.json()
                    df=pd.DataFrame(columns=['name', 'com', 'time'])
                    for post in thread['posts']:
                        for keyword in keywords:
                            if 'com' in post: 
                                if keyword in post['com']:
                                    df1=pd.Series(post)
                                    df1=df1.filter(items=['time', 'name', 'com'])
                                    df1=df1[['name', 'com', 'time']]
                                    df=df.append(df1, ignore_index=True)
                    if df.size > 3:
                        for row in df:
                            df['board']=titles[i]
                            df['organization']=group
                            df['source']="4chan"
                    with open(file, 'a') as f:
                        df.to_csv(f, header=False)
                        
attacksfile=pd.read_excel("globalterrorismdb_0919dist.xlsx")
#remove columns that won't be used in analysis
attacks=attacksfile.drop(columns = ['eventid', 'extended', 
                        'resolution', 'specificity','vicinity', 'location', 
                        'doubtterr', 'alternative_txt','success', 'natlty1', 
                        'natlty1_txt', 'targtype2','targtype2_txt', 'targsubtype2',
                        'targsubtype2_txt','corp2', 'target2', 'natlty2', 
                        'natlty2_txt', 'targtype3','targtype3_txt', 'targsubtype3', 
                        'targsubtype3_txt', 'corp3', 'target3', 'natlty3',
                        'natlty3_txt', 'gname2','gsubname2', 'gname3', 'gsubname3', 
                        'guncertain1', 'guncertain2','guncertain3', 'nperpcap', 
                        'claimed', 'claimmode','claimmode_txt', 'compclaim', 
                        'claim2','claimmode2', 'claim3', 'claimmode3', 'nkillter',
                        'nwoundte', 'property', 'propextent', 'propextent_txt',
                        'propvalue', 'propcomment', 'ishostkid', 'nhostkid',
                        'nhostkidus', 'nhours', 'ndays', 'divert', 'kidhijcountry',
                        'ransom', 'ransomamt', 'ransompaid', 'ransompaidus', 
                        'ransomnote', 'hostkidoutcome', 'hostkidoutcome_txt',
                        'nreleased', 'addnotes', 'INT_LOG', 'INT_IDEO', 'INT_MISC',
                        'INT_ANY', 'scite1', 'scite2', 'scite3', 'dbsource'])
    
#filter out attacks that took place before reddit data was available
attacks=attacks[attacks.iyear > 2005]

#clean date data
for index, row in attacks.iterrows():
    if pd.isnull(row[3]) == True:
        if row[2] == 0:
            attacks.at[index,'approxdate'] = datetime(row[0], row[1], 1)
        else:
            attacks.at[index,'approxdate'] = datetime(row[0], row[1], row[2])
    elif isinstance(row[3], datetime) == True:
        attacks.at[index,'approxdate'] = row[3]
    else:
        try:
            date=dparser.parse(row[3], fuzzy = True, default=datetime(1978, 1, 1, 0, 0))
        except ValueError:
                print(row[3])
                print("enter Month/Day/Year:")
                date=datetime.strptime(input(), '%m/%d/%Y')
        print(row[0], row[1], row[2], row[3], date)
        attacks.at[index, 'approxdate'] = date

#convert attack date to epoch time
for index, row in attacks.iterrows():
    attacks.at[index, 'epochdate']=row[3].timestamp()
    
#calculate when to start collecting social media data in epoch time
#3 months = 90 days = 77760000 seconds
for index, row in attacks.iterrows():
    attacks.at[index, 'epochstart']=row[61]-77760000
    
#convert epoch variables from float to int
attacks.epochdate=attacks.epochdate.astype(int)
attacks.epochstart=attacks.epochstart.astype(int)

groups=pd.read_excel("Organizations.xlsx")
#convert designation and removal dates to epoch time
for index, row in groups.iterrows():
    groups.at[index, 'epochdes']=row[0].timestamp()
    try: 
        groups.at[index, 'epochrem']=row[1].timestamp()
    except ValueError:
        groups.at[index, 'epochrem']=None
    
#make lists of keywords
groups.Keywords=groups.Keywords.str.split(',')

#statistical analyses
month=attacks.groupby('imonth').count()
day=attacks.groupby('iday').count()                 
stats.f_oneway(month["iyear"],month.index)
stats.f_oneway(day["iyear"],day.index)

attacktype = attacks.groupby("attacktype1").mean()
stats.f_oneway(attacktype["nkill"],attacktype.index)
stats.f_oneway(attacktype["nwound"],attacktype.index)
weapontype = attacks.groupby("weaptype1").mean()
stats.f_oneway(weapontype["nkill"],weapontype.index)
stats.f_oneway(weapontype["nwound"],weapontype.index)

#create file for social media data to go in
cols=pd.DataFrame(columns=["author", "body", "created_utc", "subreddit/board", 
                   "organization", "source"])
with open("terrorist_socialmedia.csv", "w") as f:
    cols.to_csv(f)

file="terrorist_socialmedia.csv"

#get reddit comments
groups=pd.read_excel("Organizations.xlsx")
for group in groups.iterrows():
    g=group[1]
    keywords=g[4]
    getPushshiftData(keywords[0], file)
    time.sleep(1)
    
#get 4chan threads
for g in groups.iterrows():
        group=g[1]
        get4chanData(group[2], group[4], "test.csv")
        time.sleep(1)
