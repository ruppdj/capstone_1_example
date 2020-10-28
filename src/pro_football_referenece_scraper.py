from bs4 import BeautifulSoup
from time import sleep
import requests 
from pymongo import MongoClient


def scrape_season_data(client, start_year=2005, end_year=2020, position='receiving'):
    '''
    Scrapes recever data from pro football reference and stores the data into a mongo database. It also grabs all sub recever pages for later use if needed.

    INPUT:
        client - MongoClient object - pionting to a mongo db to store pages
        start_year - int - What year to start scraping
        end_year - int - Year to stop scraping
        position - str - what position do you want to gather data for
                        options: [passing, receiving, rushing, scrimmage, defense, kicking, returns, scoring]
    '''

    db = client['nfl']
    season_collection = db['season_raw']
    player_collection = db['player_raw']
    
    num_recs = 0

    for year in range(start_year, end_year):
        url = 'https://www.pro-football-reference.com/years/{}/{}.htm#'.format(year, position)
        page = requests.get(url)
        
        # save the raw HTML so I never have to scrape again (just reparese)
        season_collection.insert_one({'year':year, 'position':position, 'html':page.text})

        soup = BeautifulSoup(page.text)

        # Get all player sub pages:
        table = soup.find('tbody')
        for row in table.find_all('tr'):
            col_one = row.find('td')
            if col_one == None:
                continue
            player = col_one.find('a')
            url_player = player.get_attribute_list('href')[0]

            num_recs += 1

            # Players will show up on different years and the sub page does not change
            # so make sure we have not scraped yet.
            if player_collection.count_documents({'player_url': url_player}) > 0:
                continue
            
            sub_url = 'https://www.pro-football-reference.com{}'.format(url_player)
            sub_page = requests.get(sub_url)
            player_collection.insert_one({'url':sub_url, 
                                      'player': url_player.split('/')[-1],
                                      'player_url': url_player,
                                      'html':sub_page.text,
                                      'position':position,
                                      'year':year})
            sleep(3)


            if num_recs%100 == 0:
                print('Year:  {}, Current Recs: {}'.format(year,num_recs))

if __name__ == '__main__':
    client = MongoClient()
    scrape_season_data(client)
    print('Recevers Scraped')
    scrape_season_data(client, position='passing')



