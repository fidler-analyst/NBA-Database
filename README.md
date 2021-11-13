# I am using MySQL Workbench to store the data in this relational database
# All data is pulled from data.nba.net API's and stored into 3 tables

#player_info table stores all of the player attributes and information, this information is retrieved by year YYYY
#game_info table stores all of the information about the games this season, this information in retrieved by date YYYYMMDD
#play_by_play table stores all of the play by play data from every game. 
    #The NBA play by play API is accessed via a 10 digit game code, which is retrieved from the game info API
    #This API also does not include player names, which is where the player_info table comes in
    
#By the end of this, you too can have a fairly inclusive data set to use for NBA analysis!


#Note: password in connection to database is not real password. Change that information to yours to replicate my results
