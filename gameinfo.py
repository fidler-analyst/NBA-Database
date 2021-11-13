#game info
 
#libraries -------------------------------------------------------------------
import pandas as pd
import urllib.request, json
import datetime
import mysql.connector
#-----------------------------------------------------------------------------

#data retrieval --------------------------------------------------------------
cnx = mysql.connector.connect(user='root', password='johnson28$',              #connect to database
                              host='127.0.0.1',
                              database='nba_data')
cursor = cnx.cursor()

#takes the second to last date this was run, that way if it was run before games were completed,
#you will retrieve all the necessary information

query = "SELECT DISTINCT startDateEastern FROM game_info" +\
    " ORDER BY startDateEastern DESC LIMIT 2 "
cursor.execute(query)

for i in cursor:
    date = i
    
lastdate = list(i)[0]                                                          #second to last date
today = datetime.date.today().strftime('%Y%m%d')

gamenum = 0 
dates = pd.date_range(start = lastdate, end = today).strftime('%Y%m%d').to_list()#list of dates to get game info from
games_df = pd.DataFrame()                                                      #dataframe to store the game information

for date in dates:
    url = 'https://data.nba.net/10s/prod/v1/'+date+'/scoreboard.json'
    with urllib.request.urlopen(url) as u:                                     #load the url
        data = json.loads(u.read().decode())                                   #store api info in data dict

    numGames = data['numGames']                                                #number of games played on that day
    games = data['games']                                                      #game from data dict
    for i in list(range(0, numGames)):
        game = games[i]
        x = list(game.items())
        y,z = zip(*x)
        y = list(y)
        z = list(z)
        
        game_df = pd.DataFrame()        
        for j in list(range(0, len(y))):
            game_df.loc[gamenum, y[j]] = str(z[j])
        
        game_df.drop(columns = ['seasonStageId', 'isGameActivated', 'statusNum', 'extendedStatusNum',
                                'startTimeUTC', 'homeStartDate', 'visitorStartDate',
                                'clock', 'watch', 'tickets', 'nugget'], inplace = True)
       
        cols = game_df.columns.to_list()
        a = 'endTimeUTC'                                                       #some games had this feature
        if a in cols:                                                           
            game_df.drop(columns = a, inplace = True)                          #if it exists, remove it
        
        games_df = pd.concat([games_df, game_df])                              #add game to games df
        gamenum+=1

games_df.reset_index(drop = True, inplace = True) 
games_df = games_df.astype(str)
game_ids = games_df.gameId                                                     #move gameId to first column
games_df.drop(columns = 'gameId', inplace = True)                              #PRIMARY key in SQL
games_df.insert(0, 'gameId', game_ids)
#-----------------------------------------------------------------------------

#upload to db ----------------------------------------------------------------
#query     
query = "INSERT IGNORE INTO game_info (seasonYear, leagueName, gameId,"+\
    "arena, startTimeEastern, startDateEastern, homeStartTime, visitorStartTime,"+\
    "gameUrlCode, isBuzzerBeater, isPreviewArticleAvail, isRecapArticleAvail,"+\
    "attendance, hasGameBookPdf, isStartTimeTBD,isNeutralVenue,"+\
    " gameDuration, period, vTeam, hTeam) VALUES(%s, %s"+\
    ",%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

for i in list(range(0, len(games_df))):

    tup = games_df.loc[i, :].to_list()
    cursor.execute(query, tup)
    cnx.commit()
        
cnx.close()                                                                    #close the connection