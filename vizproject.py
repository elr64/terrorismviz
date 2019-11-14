import pandas as pd
from scipy import stats
import requests
import json
import time
from datetime import datetime
import urllib
import dateutil.parser as dparser

def getPushshiftData(group, query, file):
    r = requests.get('https://api.pushshift.io/reddit/comment/search/',
                     params={'q': f'"{query}"', 'size': 1000})
    print(r)
    if str(r)== '<Response [429]>':
        while True:
            time.sleep(5) 
            r = requests.get('https://api.pushshift.io/reddit/comment/search/',
                     params={'q': f'"HAMAS"', 'size': 1000})
            print(r)
            if str(r) == '<Response [200]>': break
                
    d = json.loads(r.text)
    
    if len(d['data']) == 1000:
        data=d['data']
        i=d['data'][-1]
        before=i['created_utc']
        while True:
            r = requests.get('https://api.pushshift.io/reddit/comment/search/',
                     params={'q': f'"{query}"', 
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
        if row[2] != 0:
            attacks.at[index,'approxdate'] = datetime(row[0], row[1], row[2])
        else:
            try:
                start=row[3].split('-')
                date=dparser.parse(start[0], fuzzy = True, default=datetime(row[0], 1, 1, 0, 0))
            except ValueError:
                    print(row[0], row[1], row[2], row[3])
                    print("enter Month/Day/Year:")
                    date=datetime.strptime(input(), '%m/%d/%Y')
            print(row[0], row[1], row[2], row[3], date)
            attacks.at[index, 'approxdate'] = date

#convert attack date to epoch time
for index, row in attacks.iterrows():
    attacks.at[index, 'epochdate']=row[3].timestamp()

#update day
attacks['iday'] = attacks['approxdate'].dt.day
    
#calculate when to start collecting social media data in epoch time
#3 months = 90 days = 77760000 seconds
for index, row in attacks.iterrows():
    attacks.at[index, 'epochstart']=row[61]-77760000
    
#convert epoch variables from float to int
attacks.epochdate=attacks.epochdate.astype(int)
attacks.epochstart=attacks.epochstart.astype(int)

#save clean data
attacks.to_csv("terroristattacks_clean.csv")

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
monthanova=stats.f_oneway(month["iyear"],month.index)
print("One Way Anova for Month and Number of Attacks:", monthanova)
dayanova=stats.f_oneway(day["iyear"],day.index)
print("One Way Anova for Day and Number of Attacks:", dayanova)

attacktype = attacks.groupby("attacktype1").mean()
attackkill=stats.f_oneway(attacktype["nkill"],attacktype.index)
print("One Way Anova for Attack Type and Number Killed:", attackkill)
attackwound=stats.f_oneway(attacktype["nwound"],attacktype.index)
print("One Way Anova for Attack Type and Number Wounded:", attackwound)
weapontype = attacks.groupby("weaptype1").mean()
weaponkill=stats.f_oneway(weapontype["nkill"],weapontype.index)
print("One Way Anova for Weapon Type and Number Killed:", weaponkill)
weaponwound=stats.f_oneway(weapontype["nwound"],weapontype.index)
print("One Way Anova for Weapon Type and Number Killed:", weaponwound)

#create file for social media data to go in
cols=pd.DataFrame(columns=["author", "body", "created_utc", "subreddit/board", 
                   "organization", "source"])
with open("terrorist_socialmedia.csv", "w") as f:
    cols.to_csv(f)

file="terrorist_socialmedia.csv"

#get reddit comments
for group in groups.iterrows():
    g=group[1]
    group=g[2]
    keywords=g[4]
    for word in keywords:
        getPushshiftData(group, word, file)
    time.sleep(1)
    
#get 4chan threads
for g in groups.iterrows():
        group=g[1]
        get4chanData(group[2], group[4], file)
        time.sleep(1)

#read in social media data
dfin=pd.read_csv("terrorist_socialmedia.csv")

#clean social media data
df2=pd.DataFrame(columns=["author", "body", "created_utc", "subreddit/board", 
                   "organization", "source"])
for row in dfin.iterrows():
    r=row[1]
    r[2]=r[2].replace('\n','')
    r[2]=r[2].replace('\t','')
    r[2]=r[2].replace('\r','')
    r[2]=r[2].replace('""','')
    df2=df2.append(r)
#drop any lines not able to be cleaned
df2=df2.dropna()

#save clean data
df2.to_csv("clean_terroristsocialmedia.csv")