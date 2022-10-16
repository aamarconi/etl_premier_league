import requests
import json
import pandas as pd
import boto3 

def etl_pl_extract():

    #connect to aws s3
    s3 = boto3.client("s3")

    #pull premier leauge data match in 2022 season
    uri = 'https://api.football-data.org/v4/competitions/PL/matches?season=2022'
    headers = { 'X-Auth-Token': '7c4d5d35aa4f4e49aab195f600882a82' }

    respond_api = requests.get(uri, headers=headers)

    r = respond_api.json()

    data = r['matches']

    # pull premier lueage's team data in 2022 season
    uri_team = 'https://api.football-data.org/v4/competitions/PL/teams?season=2022'
    respond_api2 = requests.get(uri_team, headers=headers)

    r_team = respond_api2.json()
    team_data = r_team['teams']

    #pull fact_pl table

    pl_result = []

    for item in data:
        pl = {}
        pl['id']=item.get('id')
        pl['area_id']=item.get('area').get('id')
        pl['competition_id']=item.get('competition').get('id')
        pl['season_id']=item.get('season').get('id')
        pl['utcDate']=item.get('utcDate')
        pl['matchday']=item.get('matchday')
        pl['stage']=item.get('stage')
        pl['lastUpdated']=item.get('lastUpdated')
        pl['homeTeam_id']=item.get('homeTeam').get('id')
        pl['awayTeam_id']=item.get('awayTeam').get('id')
        pl['scoreDuration']=item.get('score').get('duration')
        pl['scoreWinner']=item.get('score').get('winner')
        pl['scoreFullTimeHome']=item.get('score').get('fullTime').get('home')
        pl['scoreFullTimeAway']=item.get('score').get('fullTime').get('away')
        pl['scoreHalftimeHome']=item.get('score').get('halfTime').get('home')
        pl['scoreHalftimeAway']=item.get('score').get('halfTime').get('away')
        pl_result.append(pl)

    fact_pl = pd.DataFrame(pl_result).set_index('id')
    fact_pl.to_csv('fact_premier_league.csv')
    s3.upload_file(Filename='fact_premier_league.csv', Bucket='premier-leauge-2022-project-raw', Key='fact_premier_league.csv')


    #pull dim_pl_team table

    team_result = []

    for item in team_data:
        tl = {}
        tl['id']=item.get('id')
        tl['name']=item.get('name')
        tl['shortName']=item.get('shortName')
        tl['tla']=item.get('tla')
        tl['crest']=item.get('crest')
        tl['address']=item.get('address')
        tl['website']=item.get('website')
        tl['founded']=item.get('founded')
        tl['clubColors']=item.get('clubColors')
        tl['venue']=item.get('venue')
        tl['coach_id']=item.get('coach').get('id')
        tl['lastUpdated']=item.get('lastUpdated')
        team_result.append(tl)

    dim_pl_team = pd.DataFrame(team_result).set_index('id')
    dim_pl_team.to_csv('dim_pl_team.csv')
    s3.upload_file(Filename='dim_pl_team.csv', Bucket='premier-leauge-2022-project-raw', Key='dim_pl_team.csv')


    #pull dim_pl_coach table

    ch_result = []

    for item in team_data:
        ch = {}
        ch['id']=item.get('coach').get('id')
        ch['firstName']=item.get('coach').get('firstName')
        ch['lastName']=item.get('coach').get('lastName')
        ch['name']=item.get('coach').get('name')
        ch['dateOfBirth']=item.get('coach').get('dateOfBirth')
        ch['nationality']=item.get('coach').get('nationality')
        ch_result.append(ch)

    dim_pl_coach = pd.DataFrame(ch_result).set_index('id')
    dim_pl_coach.to_csv('dim_pl_coach.csv')
    s3.upload_file(Filename='dim_pl_coach.csv', Bucket='premier-leauge-2022-project-raw', Key='dim_pl_coach.csv')


    #pull dim_competition table

    ct_result = []

    for item in data:
        ct = {}
        ct['id']=item.get('competition').get('id')
        ct['name']=item.get('competition').get('name')
        ct['code']=item.get('competition').get('code')
        ct['type']=item.get('competition').get('type')
        ct['emblem']=item.get('competition').get('emblem')
        ct_result.append(ct)

    dim_competition = pd.DataFrame(ct_result).set_index('id')
    dim_competition.to_csv('dim_competition.csv')
    s3.upload_file(Filename='dim_competition.csv', Bucket='premier-leauge-2022-project-raw', Key='dim_competition.csv')


    #pull dim_area table

    ar_result = []

    for item in data:
        ar = {}
        ar['id']=item.get('area').get('id')
        ar['name']=item.get('area').get('name')
        ar['code']=item.get('area').get('code')
        ar['flag']=item.get('area').get('flag')
        ar_result.append(ar)

    dim_area = pd.DataFrame(ar_result).set_index('id')
    dim_area.to_csv('dim_area.csv')
    s3.upload_file(Filename='dim_area.csv', Bucket='premier-leauge-2022-project-raw', Key='dim_area.csv')

    #pull dim_season table

    ss_result = []

    for item in data:
        ss = {}
        ss['id']=item.get('season').get('id')
        ss['startDate']=item.get('season').get('startDate')
        ss['currentMatchday']=item.get('season').get('currentMatchday')
        ss['winner']=item.get('season').get('winner')
        ss_result.append(ss)

    dim_season = pd.DataFrame(ss_result).set_index('id')
    dim_season.to_csv('dim_season.csv')
    s3.upload_file(Filename='dim_season.csv', Bucket='premier-leauge-2022-project-raw', Key='dim_season.csv')