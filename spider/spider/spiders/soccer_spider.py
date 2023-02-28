import re
import scrapy
import pandas as pd
import numpy as np
import os

class GameSpider(scrapy.Spider):
    name = 'game'

    start_urls = [
        'https://fbref.com/en/matches/47134469/Almeria-Barcelona-February-26-2023-La-Liga'
    ]

    def parse(self, response):
        df = None

        for i in range(1,3):
            team_id = re.findall(
                '(?<=/en/squads/)[a-z0-9A-Z]+', 
                response.xpath(
                    f'//div[@id="content"]/div[@class="scorebox"]/div[{i}]/div[1]/strong/a/@href'
                ).get()
            )[0]
            team_name = response.xpath(
                f'//div[@id="content"]/div[@class="scorebox"]/div[{i}]/div[1]/strong/a/text()'
            ).get()
            team_possession = float(response.xpath(f'//div[@id="team_stats"]/table/tr[3]/td[{i}]/div/div/strong/text()').get()[:-1])/100

            table = response.xpath(
                f'//table[@id="stats_{team_id}_summary"]'
            ).get()
            team_df = pd.read_html(table)[0]
            team_df.columns = team_df.columns.droplevel()
            team_df.loc[len(team_df)-1, 'Player'] = 'Totals'
            team_df.loc[:, 'Team'] = team_name
            team_df.loc[:, 'Possession'] = team_possession

            if df is not None:
                df = pd.concat([df, team_df])
            else:
                df = team_df.copy()

        club_totals = df[df['Player'] == 'Totals'].reset_index(drop=True)
        df = df[df['Player'] != 'Totals'].reset_index(drop=True)

        df.to_csv('../player_summary_stats.csv', index=False)
        club_totals.to_csv('../team_summary_stats.csv', index=False)


# =================================================================================================


class FCBSpider(scrapy.Spider):
    name = 'fcb'
    played = {
        'Pedri': [],
        'Ousmane Dembélé': []
    }
    fcb_df = None
    k = 0

    start_urls = [
        'https://fbref.com/en/squads/206d90db/2022-2023/all_comps/Barcelona-Stats-All-Competitions'
    ]

    def parse(self, response):
        match_report_links = response.xpath(
            '//table[@id="matchlogs_for"]/tbody/tr/td[@data-stat="match_report"]/a'
        )
        yield from response.follow_all(match_report_links, self.parse_game)


    def parse_game(self, response):
        if 'matchup' in response.url:
            return
        df = None

        match_id = re.findall(
            '(?<=en/matches/)[a-z0-9A-Z]+', 
            response.url
        )[0]

        print(f"\n{match_id}\n")

        for i in range(1,3):
            team_id = re.findall(
                '(?<=/en/squads/)[a-z0-9A-Z]+', 
                response.xpath(
                    f'//div[@id="content"]/div[@class="scorebox"]/div[{i}]/div[1]/strong/a/@href'
                ).get()
            )[0]
            team_name = response.xpath(
                f'//div[@id="content"]/div[@class="scorebox"]/div[{i}]/div[1]/strong/a/text()'
            ).get()

            team_possession = float(response.xpath(f'//div[@id="team_stats"]/table/tr[3]/td[{i}]/div/div/strong/text()').get()[:-1])/100

            table = response.xpath(
                f'//table[@id="stats_{team_id}_summary"]'
            ).get()
            team_df = pd.read_html(table)[0]
            team_df.columns = team_df.columns.droplevel()
            team_df.loc[len(team_df)-1, 'Player'] = 'Totals'
            team_df.loc[:, 'Team'] = team_name
            team_df.loc[:, 'Possession'] = team_possession

            if df is not None:
                df = pd.concat([df, team_df])
            else:
                df = team_df.copy()

        teams = df[df['Player'] == 'Totals'].reset_index(drop=True)
        teams.loc[:, 'Match'] = match_id

        for player in self.played:
            if player in df['Player'].tolist():
                self.played[player].append(match_id)
            else:
                self.played[player].append('')
        
        tkl = 'Tkl' if 'Tkl' in teams.columns else 'TklW'
        teams = teams[['Sh', 'SoT', tkl, 'Int', 'Gls', 'Player', 'Team', 'Match', 'Possession']]
        if 'fcb.csv' in os.listdir('..'):
            teams.to_csv("../fcb.csv", mode='a', index=False, header=False)
        else:
            teams.to_csv('../fcb.csv', index=False)
        

    def close(self,reason):
        players = pd.DataFrame(self.played)
        players.to_csv('../pedri_dembele.csv', index=False)