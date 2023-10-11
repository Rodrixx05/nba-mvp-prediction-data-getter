from unidecode import unidecode
from bs4 import BeautifulSoup, Comment
import requests
import pandas as pd
import numpy as np

class BasketballReferenceGetter():
    """
    Object that extracts players' individual stats for the given seasons from Basketball Reference
    using WebScraping techniques.
    The object returns a DataFrame for the specified request
    """
    def __init__(self):
        pass

    """
    Protected Functions
    """
    def _create_ranks(self, df, col_start, col_end = None):
        """
        Adds new columns to the passed dataframe corresponding to the descending ranking of the selected columns
        """
        df_ranks = df.iloc[:, col_start:col_end].rank(method = 'dense', ascending = False)
        df_ranks = df_ranks.add_suffix('_rank').astype(int)
        return df.join(df_ranks)     

    def _get_team_record(self, team, year):
        """
        Extracts the team record from the given season, in the form of W-L
        """
        url = f'https://www.basketball-reference.com/teams/{team}/{year}.html'
        response = requests.get(url)
        if response.status_code != 200:
            return response.status_code
        soup = BeautifulSoup(response.text, features = "lxml")
        page_body = soup.find('div', {'data-template': 'Partials/Teams/Summary'})
        record = page_body.find('p').text.split()[1].strip(',')
        return record
    
    def _get_season_records(self, year): 
        """
        Given a dataframe with teams and seasons, it returns a dataframe 
        containing the season record for each team as the %W column, and also the total games played as GT
        """
        url = f"https://www.basketball-reference.com/leagues/NBA_{year}_standings.html"
        response = requests.get(url)
        if response.status_code != 200:
            return response.status_code
        soup_html = BeautifulSoup(response.text, features = "lxml")
        for comment in soup_html.find_all(text = lambda text: isinstance(text, Comment)):
            if comment.find("expanded_standings") > 0:
                soup_div = BeautifulSoup(comment, features = "lxml")
                break
        table_body = soup_div.find('table', {'id': 'expanded_standings'}).find('tbody')
        rows = table_body.find_all('tr')
        standings = []
        for row in rows:
            team = row.find('td', {'data-stat': 'team_name'}).find('a', href = True)['href'].split('/')[2]
            record = row.find('td', {'data-stat': 'Overall'}).text
            standings.append([team, record])
        df_season_records = pd.DataFrame(standings, columns = ['Tm', 'Record'])
        df_season_records['W'] = df_season_records['Record'].apply(lambda x: x.split('-')[0]).astype(int)
        df_season_records['L'] = df_season_records['Record'].apply(lambda x: x.split('-')[1]).astype(int)
        df_season_records['%W'] = round(df_season_records['W'] / (df_season_records['W'] + df_season_records['L']), 3)
        df_season_records['GT'] = df_season_records['W'] + df_season_records['L']
        df_season_records.drop(columns = ['Record', 'W', 'L'], inplace = True)
        return df_season_records
    
    def _fillna_tot_team(self, series, df):
        """
        It fills the team record column and the total games column 
        for players who have played for more than one team.
        For the team record, it does the weighted average. For the total games, it takes the max.
        """
        if pd.isna(series['%W']):
            tot_gp = series['G']
            sub_df = df[(df['Rk'] == series['Rk']) & (df['Tm'] != 'TOT')]
            new_pct = round(((sub_df['G'] * sub_df['%W']).sum() / tot_gp), 3)
            series['%W'] = new_pct
            series['GT'] = sub_df['GT'].max()
        return series
    
    def _years_list(self, object):
        """
        Returns a list of integers according to the passed object, in order to loop over seasons.
        The idea is to let the extractor functions accept an integer, a float, a list of numbers
        or a string representing a range of seasons.
        """
        if type(object) is int or type(object) is float:
            return [object]
        elif type(object) is str:
            val_range = object.split('-')
            return range(int(val_range[0]), int(val_range[1]) + 1)
        else:
            return object


    """
    Public Functions
    """
    def extract_player_stats_pg(self, years, ranks = False):
        return_list = []
        for year in self._years_list(years):
            url = f'https://www.basketball-reference.com/leagues/NBA_{year}_per_game.html'
            response = requests.get(url)
            if response.status_code != 200:
                return response.status_code
            
            soup = BeautifulSoup(response.text, features = "lxml")
            table_header = soup.find('table', {'id': 'per_game_stats'}).find('thead')
            header = [row.text for row in table_header.find_all('th')]

            table_body = soup.find('table', {'id': 'per_game_stats'}).find('tbody')
            rows = table_body.find_all('tr', {'class': ['full_table', 'italic_text partial_table']})
            players = []
            for row in rows:
                player_data = [stat.text for stat in row.find_all(['td', 'th'])]
                players.append(player_data)

            df_player_stats_pg = pd.DataFrame(players)
            df_player_stats_pg.columns = header

            df_player_stats_pg['GS'].replace('', '-10', inplace = True)
            df_player_stats_pg.replace('', '0', inplace = True)

            df_player_stats_pg = df_player_stats_pg.apply(pd.to_numeric, errors = 'ignore')

            df_player_stats_pg['Player'] = df_player_stats_pg['Player'].str.strip('*')

            if ranks:
                df_player_stats_pg = self._create_ranks(df_player_stats_pg, 7)

            df_player_stats_pg['%GS'] = np.where(df_player_stats_pg['GS'] >= 0, round(df_player_stats_pg['GS'] / df_player_stats_pg['G'], 3), -1)

            df_player_stats_pg['Season'] = year

            return_list.append(df_player_stats_pg)
            
        return pd.concat(return_list, ignore_index = True)

    def extract_player_stats_totals(self, years, ranks = False):
        return_list = []
        for year in self._years_list(years):
            url = f'https://www.basketball-reference.com/leagues/NBA_{year}_totals.html'
            response = requests.get(url)
            if response.status_code != 200:
                return response.status_code
            
            soup = BeautifulSoup(response.text, features = "lxml")
            table_header = soup.find('table', {'id': 'totals_stats'}).find('thead')
            header = [row.text for row in table_header.find_all('th')]

            table_body = soup.find('table', {'id': 'totals_stats'}).find('tbody')
            rows = table_body.find_all('tr', {'class': ['full_table', 'italic_text partial_table']})
            players = []
            for row in rows:
                player_data = [stat.text for stat in row.find_all(['td', 'th'])]
                players.append(player_data)

            df_player_stats_totals = pd.DataFrame(players)
            df_player_stats_totals.columns = header

            df_player_stats_totals['GS'].replace('', '-10', inplace = True)
            df_player_stats_totals.replace('', '0', inplace = True)

            df_player_stats_totals = df_player_stats_totals.apply(pd.to_numeric, errors = 'ignore')

            df_player_stats_totals['Player'] = df_player_stats_totals['Player'].str.strip('*')

            if ranks:
                df_player_stats_totals = self._create_ranks(df_player_stats_totals, 7)

            df_player_stats_totals['Season'] = year

            return_list.append(df_player_stats_totals)
            
        return pd.concat(return_list, ignore_index = True)
    
    def extract_player_stats_advanced(self, years, ranks = False):
        return_list = []
        for year in self._years_list(years):
            url = f'https://www.basketball-reference.com/leagues/NBA_{year}_advanced.html'
            response = requests.get(url)
            if response.status_code != 200:
                return response.status_code
            
            soup = BeautifulSoup(response.text, features = "lxml")
            table_header = soup.find('table', {'id': 'advanced_stats'}).find('thead')
            header = [row.text for row in table_header.find_all('th')]

            table_body = soup.find('table', {'id': 'advanced_stats'}).find('tbody')
            rows = table_body.find_all('tr', {'class': ['full_table', 'italic_text partial_table']})
            players = []
            for row in rows:
                player_data = [stat.text for stat in row.find_all(['td', 'th'])]
                players.append(player_data)

            df_player_stats_advanced = pd.DataFrame(players)
            df_player_stats_advanced.columns = header

            df_player_stats_advanced.drop(columns = '\xa0', inplace = True)

            df_player_stats_advanced.replace('', '0', inplace = True)

            df_player_stats_advanced = df_player_stats_advanced.apply(pd.to_numeric, errors = 'ignore')

            df_player_stats_advanced['Player'] = df_player_stats_advanced['Player'].str.strip('*')

            df_player_stats_advanced.loc[:, 'ORB%':'USG%'] = df_player_stats_advanced.loc[:, 'ORB%':'USG%'] / 100

            if ranks:
                df_player_stats_advanced = self._create_ranks(df_player_stats_advanced, 6)

            df_player_stats_advanced['Season'] = year

            return_list.append(df_player_stats_advanced)
            
        return pd.concat(return_list, ignore_index = True)

    def extract_mvp_votes(self, years):
        return_list = []
        for year in self._years_list(years):
            url = f'https://www.basketball-reference.com/awards/awards_{year}.html#mvp'
            response = requests.get(url)
            if response.status_code != 200:
                return response.status_code

            soup = BeautifulSoup(response.text, features = "lxml")
            table_body = soup.find('table', {'id': 'mvp'}).find('tbody')
            rows = table_body.find_all('tr')

            mvp_votes = []
            for row in rows:
                player_data = []
                player = row.find('td').find('a').text
                player_data.append(player)
                votes = row.find('td', {'data-stat': 'points_won'}).text
                player_data.append(int(float(votes)))
                percentage = row.find('td', {'data-stat': 'award_share'}).text
                player_data.append(float(percentage))
                max = row.find('td', {'data-stat': 'points_max'}).text
                player_data.append(int(max))
                mvp_votes.append(player_data)
                
            df_mvp_votes = pd.DataFrame(mvp_votes, columns = ['Player', 'Votes', 'Share', 'MaxVotes'])

            df_mvp_votes['Season'] = year

            return_list.append(df_mvp_votes)

        return pd.concat(return_list, ignore_index = True)
    
    def extract_player_stats_multiple(self, years, totals = True,  mvp = True, team_stats = True, advanced = False, ranks = False):
        return_list = []
        for year in self._years_list(years):
            df_return = self.extract_player_stats_pg(year, ranks)
            
            if totals:
                df_tot = self.extract_player_stats_totals(year, ranks)
                df_tot.drop(columns = [col for col in df_tot.columns if col.startswith(('FG%', '3P%', '2P%', 'eFG%', 'FT%'))], inplace = True)
                df_return = pd.merge(left = df_return, right = df_tot, how = 'inner', on = ['Rk', 'Player', 'Pos', 'Age', 'Tm', 'G', 'GS', 'Season'], suffixes = ['_pg', '_tot'])

            if advanced:
                df_adv = self.extract_player_stats_advanced(year, ranks)
                df_adv.drop(columns = [col for col in df_tot.columns if col.startswith('MP')], inplace = True)
                df_return = pd.merge(left = df_return, right = df_adv, how = 'inner', on = ['Rk', 'Player', 'Pos', 'Age', 'Tm', 'G', 'Season'])
            
            if team_stats:
                df_records = self._get_season_records(year)
                df_return = pd.merge(left = df_return, right = df_records, how = 'left', on = 'Tm')
                df_return = df_return.apply(self._fillna_tot_team, axis = 1, args = (df_return, ))
                if ranks:
                    index_w = df_return.columns.get_loc('%W')
                    df_return = self._create_ranks(df_return, index_w, index_w + 1)
                df_return['GT'] = df_return['GT'].astype(int)
                df_return['%G'] = round(df_return['G'] / df_return ['GT'], 3)

            if mvp:
                df_mvp = self.extract_mvp_votes(year)
                df_return = pd.merge(left = df_return, right = df_mvp, how = 'left', on = ['Player', 'Season'])
                df_return.fillna({'Votes': 0, 'Share': 0, 'MaxVotes': df_return['MaxVotes'].max()}, inplace = True)
                df_return[['Votes', 'MaxVotes']] = df_return[['Votes', 'MaxVotes']].astype(int)
                
            return_list.append(df_return)
        
        return pd.concat(return_list, ignore_index = True)