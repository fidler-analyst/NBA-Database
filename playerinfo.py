#player info
 
#libraries -------------------------------------------------------------------
import pandas as pd
import urllib.request, json
import mysql.connector
#-----------------------------------------------------------------------------
 
#read in ---------------------------------------------------------------------
url = 'https://data.nba.net/10s/prod/v1/2021/players.json'                     #url to api
with urllib.request.urlopen(url) as u:                                         #load the url
    data = json.loads(u.read().decode())                                       #store api info in data dict
   
league = data['league']                                                        #opens 'league' key
standard = league['standard']                                                  #opens 'standard' key
player_info = pd.DataFrame(standard)                                           #puts nba player data into player_info
#-----------------------------------------------------------------------------
 
#data manipulation -----------------------------------------------------------
draft_info = player_info.draft.apply(pd.Series)                                #splits draft info which was still in key value pairs
draft_info.rename(columns = {'teamId':'draft_teamId', 'seasonYear': 'draftYear'}, inplace = True)
 
player_info.drop(columns = ['teamSitesOnly', 'teams', 'draft'], inplace = True)
player_info = pd.concat([player_info, draft_info], axis = 1)                   #add draft information to player_info
 
actives = player_info[player_info.isActive == True]                            #only active players
actives = actives.astype(str)
#-----------------------------------------------------------------------------
 
#upload to database ----------------------------------------------------------
cnx = mysql.connector.connect(user='root', password='password',                #connect to database
                              host='127.0.0.1',
                              database='nba_data')
cursor = cnx.cursor()

#SQL INSERT statement
query = "INSERT IGNORE INTO player_info (firstName, lastName, temporaryDisplayName," +\
        "personId, teamId, jersey, isActive, pos, heightFeet, heightInches,"  +\
        "heightMeters, weightPounds, weightKilograms, dateOfBirthUTC,"        +\
        "nbaDebutYear, yearsPro, collegeName, lastAffiliation, country,"      +\
        "isallStar, draft_teamId, pickNum, roundNum, draftYear) VALUES(%s, %s"+\
        ",%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s" +\
        ", %s, %s, %s, %s)"

#evry row in the actives dataframe is uploaded to the data base
for i in list(range(0, len(actives))):
    tup = actives.loc[i, :].to_list()
    cursor.execute(query, tup)
    cnx.commit()

cnx.close()                                                                    #close the connection
