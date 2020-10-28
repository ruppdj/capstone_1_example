from pymongo import MongoClient
from bs4 import BeautifulSoup

def clean_scrape_data(client):
    '''
    Takes a connection to a mongo db and cleans scraped pages and stores them into 
    a new collection.  A check is done to make sure we do not enter duplicates.
    '''

    db = client['nfl']
    season_collection = db['season_raw']
    player_collection = db['player_raw']
    clean_player = db['clean_player']

    for season_rec in season_collection.find({},{'_id':0}):
        

        year = season_rec['year']
        position = season_rec['position']
        
        soup = BeautifulSoup(season_rec['html']) 
        
        # Get the table data so we can ge sub data:
        table = soup.find_all('tbody')[0]
        for row in table.find_all('tr'):
            
            # make sure it is not a blank row.
            col_one = row.find('td')
            if col_one == None:
                continue
                
            # Get player url extension to find record in mongo
            player = col_one.find('a')
            url_player = player.get_attribute_list('href')[0]
            
            
            # Turn sub page inot a soup object
            sub_rec = player_collection.find_one({'player_url':url_player},{'_id':0})
            
            # This should not happen but if it does then skip.
            if sub_rec == None:
                continue

            sub_soup = BeautifulSoup(sub_rec['html'])
            
            
            data = {'position':position, 'year': year, 'player_url':url_player}
            
            # Get all row data Cool fact is that this page has a parameter we can use to name the stats
            for col in row.find_all('td'):
                field = col.get('data-stat')
                value = col.text
                data[field] = value
                
            # Get height and weight from sub page
            height = sub_soup.find('span',{'itemprop':'height'}).text
            weight = sub_soup.find('span',{'itemprop':'weight'}).text
            
            data['heigh'] = height
            data['weight'] = weight
            
            
            
            # Put clean record in new collection.  We will check to not make duplicates
            if clean_player.count_documents(data) == 0:
                clean_player.insert_one(data)



if __name__=='__main__':
    client = MongoClient()
    clean_scrape_data(client)