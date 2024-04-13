''' 
To load and parse Statsbomb json data into a Postgresql database 
for further analysis purposes

The combined files created in the previous step (sb_events.json, sb_lineups.json, sb_matches.json)
together with competitions.json should be ready in json_loader/statsbomb under the project folder
 
This code will
   1. Load the competitions.json and sb_*.json data files into temporary tables in the database
   2. Create database schema to hold statsbomb data
   3. Parse sb raw data and populate database tables with relevant data

'''

import psycopg
import json
import traceback

#-----------------------------------------------------------------------
# To create tables required for the project
# Existing tables with same name will be deleted 
#   conn: connection to the database
#-----------------------------------------------------------------------
def create_db_schema(conn):

    conn.execute("DROP TABLE IF EXISTS  event_related")
    conn.execute("DROP TABLE IF EXISTS  event_tactics")
    conn.execute("DROP TABLE IF EXISTS  events")
    conn.execute("DROP TABLE IF EXISTS  tmp_event_data")
    conn.execute("DROP TABLE IF EXISTS  tmp_event_main")
    conn.execute("DROP TABLE IF EXISTS  managers")
    conn.execute("DROP TABLE IF EXISTS  players")
    conn.execute("DROP TABLE IF EXISTS  matches")
    conn.execute("DROP TABLE IF EXISTS  persons")
    conn.execute("DROP TABLE IF EXISTS  teams")
    conn.execute("DROP TABLE IF EXISTS  competitions")
    conn.execute("DROP TABLE IF EXISTS  seasons")
    conn.execute("DROP TABLE IF EXISTS  stadiums")
    conn.execute("DROP TABLE IF EXISTS  countries")

    # create countries
    print('----- creating table countries...')
    qry = '''
        CREATE TABLE IF NOT EXISTS countries 
        ( country_id          integer       primary key
        , country_name        varchar(32)   unique not null
        );
    '''
    conn.execute(qry)

    # create stadiums
    print('----- creating table stadiums...')
    qry = '''
        CREATE TABLE IF NOT EXISTS stadiums 
        ( stadium_id    integer     primary key
        , stadium_name	varchar(64) unique not null
        , country_id	integer
        , foreign key(country_id)   references countries
        );
    '''
    conn.execute(qry)

    # create seasons
    print('----- creating table seasons...')
    qry = '''
        CREATE TABLE IF NOT EXISTS seasons 
        ( season_id    integer     primary key
        , season_name  varchar(32) unique not null
        );
    '''
    conn.execute(qry)

    # create competitions
    print('----- creating table competitions...')
    qry = '''
        CREATE TABLE IF NOT EXISTS competitions 
        ( competition_id    integer     primary key
        , competition_name	varchar(32) unique not null
        , gender	        varchar(6)  check (gender = 'male' or gender = 'female')
        , youth	            boolean
        , international	    boolean
        , country_name      varchar(32)
        );
    '''
    conn.execute(qry)

    # create persons
    print('----- creating table persons...')
    qry = '''
        CREATE TABLE IF NOT EXISTS persons
        ( id	        integer     primary key
        , name	        varchar(64) not null
        , nickname      varchar(32)
        , dob           date
        , country_id	integer
        , foreign key (country_id)   references countries
        );
    '''
    conn.execute(qry)

    # create teams
    print('----- creating table teams...')
    qry = '''
        CREATE TABLE IF NOT EXISTS teams
        ( team_id       integer     primary key
        , team_name	    varchar(32) unique not null
        , gender	    varchar(6)  check (gender in ('male','female'))
        , country_id	integer
        , foreign key (country_id)   references countries
        );
    '''
    conn.execute(qry)

    # create matches
    print('----- creating table matches...')
    qry = '''
        CREATE TABLE IF NOT EXISTS matches  
        ( match_id	        integer     primary key
        , competition_id	integer     not null
        , season_id	        integer     not null
        , competition_name	varchar(32) not null
        , season_name       varchar(32) not null
        , match_date	    date        not null
        , kick_off	        time
        , stadium_id	    integer
        , referee_id	    integer
        , home_team_id	    integer     not null
        , away_team_id	    integer     not null
        , home_team_group	varchar(32)
        , away_team_group	varchar(32)
        , home_score	    integer
        , away_score	    integer
        , match_week	    integer
        , competition_stage	varchar(32)
        , foreign key (competition_id) references competitions
        , foreign key (season_id) references seasons
        , foreign key (stadium_id) references stadiums
        , foreign key (referee_id) references persons(id)
        , foreign key (home_team_id) references teams
        , foreign key (away_team_id) references teams
        );
    '''
    conn.execute(qry)

    # create managers (relationship)
    print('----- creating table managers...')
    qry = '''
        CREATE TABLE IF NOT EXISTS managers
        ( manager_id    integer     
        , team_id	    integer 
        , match_id	    integer         
        , primary key (manager_id, team_id, match_id)
        , foreign key (manager_id) references persons(id)
        , foreign key (team_id)    references teams
        , foreign key (match_id)   references matches
        );
    '''
    conn.execute(qry)

    # create players (relationships)
    print('----- creating table players...')
    qry = '''
        CREATE TABLE IF NOT EXISTS players
        ( player_id	    integer
        , team_id	    integer 
        , match_id	    integer 
        , jersey_number integer
        , primary key (player_id, team_id, match_id)
        , foreign key (player_id) references persons(id)
        , foreign key (team_id)   references teams
        , foreign key (match_id)  references matches
        );
    '''
    conn.execute(qry)

    # create tmp_event_main
    print('----- creating table tmp_event_main...')
    qry = '''
        CREATE TABLE IF NOT EXISTS tmp_event_main 
        ( event_id          uuid            primary key
        , index	            integer	
        , period	        integer	
        , timestamp	        time	
        , minute	        integer	
        , second	        integer	
        , type	            varchar(32)
        , possession	    integer	
        , possession_team_id integer	
        , play_pattern	    jsonb
        , team_id           integer
        , player_id	        integer	
        , position          varchar(32)	
        , location	        decimal []
        , duration          decimal	
        , under_pressure	boolean	
        , off_camera	    boolean	
        , out	            boolean	
        , match_id          integer	
        );
    '''
    conn.execute(qry)

    # create tmp_event_data
    print('----- creating table tmp_event_data...')
    qry = '''
        CREATE TABLE IF NOT EXISTS tmp_event_data
        ( event_id             uuid        primary key
        , _advantage           boolean
        , _aerial_won          boolean
        , _angle               decimal
        , _assisted_shot_id    uuid
        , _backheel            boolean
        , _body_part           varchar(32)
        , _card                varchar(32)
        , _counterpress        boolean
        , _cross               boolean
        , _cut_back            boolean
        , _defensive           boolean
        , _deflected           boolean
        , _deflection          boolean
        , _early_video_end     boolean
        , _end_location        decimal []
        , _first_time          boolean
        , _follows_dribble     boolean
        , _freeze_frame        jsonb
        , _goal_assist         boolean
        , _height              varchar(32)
        , _in_chain            boolean
        , _key_pass_id         uuid
        , _late_video_start    boolean
        , _length              decimal
        , _match_suspended     boolean
        , _miscommunication    boolean
        , _no_touch            boolean
        , _nutmeg              boolean
        , _offensive           boolean
        , _open_goal           boolean
        , _outcome             varchar(32)
        , _overrun             boolean
        , _penalty             boolean
        , _permanent           boolean
        , _position            varchar(32)
        , _recipient_id        integer
        , _recovery_failure    boolean
        , _replacement_id      integer
        , _save_block          boolean
        , _shot_assist         boolean
        , _statsbomb_xg        decimal
        , _switch              boolean
        , _technique           varchar(32)
        , _type                varchar(32)
        );
    '''
    conn.execute(qry)

    # related_events and tactics are multi values columns so they are
    # implemented as separate tables (event_related_events & event_tactics)
    # in the parsing phase

    # create events
    # qry = '''
    #     CREATE TABLE IF NOT EXISTS events
    #     ( event_id          uuid        primary key
    #     , index	            integer	
    #     , period	        integer	
    #     , timestamp	        time	
    #     , minute	        integer	
    #     , second	        integer	
    #     , event_type	    varchar(32)
    #     , possession	    integer	
    #     , possession_team_id integer	
    #     , play_pattern	    jsonb
    #     , team_id           integer
    #     , player_id	        integer	
    #     , position          varchar(32)	
    #     , location	        decimal []
    #     , duration          decimal	
    #     , under_pressure	boolean	
    #     , off_camera	    boolean	
    #     , out	            boolean	
    #     , related_events	jsonb	
    #     , tactics	        jsonb	
    #     , match_id          integer	
    #     , _advantage           boolean
    #     , _aerial_won          boolean
    #     , _angle               decimal
    #     , _assisted_shot_id    uuid
    #     , _backheel            boolean
    #     , _body_part           varchar(32)
    #     , _card                varchar(32)
    #     , _counterpress        boolean
    #     , _cross               boolean
    #     , _cut_back            boolean
    #     , _defensive           boolean
    #     , _deflected           boolean
    #     , _deflection          boolean
    #     , _early_video_end     boolean
    #     , _end_location        decimal []
    #     , _first_time          boolean
    #     , _follows_dribble     boolean
    #     , _freeze_frame        jsonb
    #     , _goal_assist         boolean
    #     , _height              varchar(32)
    #     , _in_chain            boolean
    #     , _key_pass_id         uuid
    #     , _late_video_start    boolean
    #     , _length              decimal
    #     , _match_suspended     boolean
    #     , _miscommunication    boolean
    #     , _no_touch            boolean
    #     , _nutmeg              boolean
    #     , _offensive           boolean
    #     , _open_goal           boolean
    #     , _outcome             varchar(32)
    #     , _overrun             boolean
    #     , _penalty             boolean
    #     , _permanent           boolean
    #     , _position            varchar(32)
    #     , _recipient_id        integer
    #     , _recovery_failure    boolean
    #     , _replacement_id      integer
    #     , _save_block          boolean
    #     , _shot_assist         boolean
    #     , _statsbomb_xg        decimal
    #     , _switch              boolean
    #     , _formation           integer
    #     , _lineup              jsonb
    #     , _technique           varchar(32)
    #     , _type                varchar(32)
    #     , foreign key (match_id)            references matches
    #     , foreign key (team_id)             references teams
    #     , foreign key (player_id)           references persons
    #     , foreign key (possession_team_id)  references teams
    #     , foreign key (_recipient_id)       references persons
    #     , foreign key (_replacement_id)     references persons
    #     );
    # '''
    # conn.execute(qry)

    conn.commit()


#-----------------------------------------------------------------------
# To import json data into a table having a single column named 'data' 
# with type of jsonb. This is a helper funciton for import_sbdata()
#   conn: connection to the database
#   file_path: json file to load
#   table_name: table to store raw json data 
#-----------------------------------------------------------------------
def import_json_file(conn, file_path, table_name):
    with open(file_path,"r", encoding = 'utf-8') as json_file:
        data = json.load(json_file)
        print('       # json entries:',len(data))
        with conn.cursor() as cur:
            cur.executemany(f'INSERT INTO {table_name} (data) VALUES(%s)', [(json.dumps(d),) for d in data])
            print('       # records loaded:',table_name,cur.rowcount)
        

#-----------------------------------------------------------------------
# To import sb json data into "temporary" tables named sb_<name>
#   these sb tables can be dropped after the parsing process complete
#   conn =  connection to the database
#-----------------------------------------------------------------------
def import_sbdata(conn):
    # create temporary tables to hold statsbomb raw data
    conn.execute("DROP TABLE IF EXISTS sb_competitions")
    conn.execute("DROP TABLE IF EXISTS sb_lineups")
    conn.execute("DROP TABLE IF EXISTS sb_matches")
    conn.execute("DROP TABLE IF EXISTS sb_events")
    conn.execute("CREATE TABLE sb_competitions (data jsonb)")
    conn.execute("CREATE TABLE sb_lineups (data jsonb)")
    conn.execute("CREATE TABLE sb_matches (data jsonb)")
    conn.execute("CREATE TABLE sb_events (data jsonb)")

    # populate table sb_competitions
    print("----- loading statsbomb/competitions.json...")
    import_json_file(conn, file_path="statsbomb/competitions.json", table_name="sb_competitions")

    # populate table sb_lineups
    print("----- loading statsbomb/sb_lineups.json...")
    import_json_file(conn, file_path="statsbomb/sb_lineups.json", table_name="sb_lineups")

    # populate table sb_matches
    print("----- loading statsbomb/matches.json...")
    import_json_file(conn, file_path="statsbomb/sb_matches.json", table_name="sb_matches")

    # populate table sb_events
    print("----- loading statsbomb/events.json...")
    import_json_file(conn, file_path="statsbomb/sb_events.json", table_name="sb_events")

    conn.commit()


#-----------------------------------------------------------------------------
# To extract data from sb tables, transform and load to master data tables
#   conn =  connection to the database
#-----------------------------------------------------------------------------
def parse_sbdata(conn):     
    # load country data
    print('----- populating table countries...')
    str = '''
        INSERT INTO countries (country_id, country_name)
            (SELECT DISTINCT 
                (country->>'id')::int
                ,country->>'name'
            FROM sb_lineups,
                jsonb_to_recordset(data->'lineup') country(country jsonb)
            ORDER BY 2
            )
        '''
    print(f'      {conn.execute(str).rowcount} records loaded')

    # load_stadiums
    print('----- populating table stadiums...')
    str = '''
    INSERT INTO stadiums (stadium_id, stadium_name, country_id)
        (SELECT DISTINCT
            (data->'stadium'->>'id')::int,
            data->'stadium'->>'name',
            (data->'stadium'->'country'->>'id')::int
        FROM sb_matches
        WHERE data->'stadium'->>'id' IS NOT NULL
        )
    '''
    print(f'      {conn.execute(str).rowcount} records loaded')

    # load competitions data
    print('----- populating table competitions...')
    str = '''
    INSERT INTO competitions (competition_id, competition_name, gender, youth, international, country_name)
        (SELECT DISTINCT
            (data->>'competition_id')::int
            ,data->>'competition_name'
            ,data->>'competition_gender'
            ,(data->>'competition_youth')::boolean
            ,(data->>'competition_international')::boolean
            ,data->>'country_name'
        FROM sb_competitions
        )
    '''
    print(f'      {conn.execute(str).rowcount} records loaded')

    # load seasons data
    print('----- populating table seasons...')
    str = '''
    INSERT INTO seasons (season_id, season_name)
        (SELECT DISTINCT 
            (data->>'season_id')::int
            ,data ->>'season_name'
        FROM sb_competitions
        )
    '''            
    print(f'      {conn.execute(str).rowcount} records loaded')

    # load players from lineups into persons
    print('----- populating table persons with players data...')
    str = '''
    INSERT INTO persons (id, name, nickname, country_id)
    (SELECT DISTINCT 
        lineup.player_id
        ,lineup.player_name
        ,lineup.player_nickname
        ,(country->>'id')::int
    FROM sb_lineups,
    JSONB_TO_RECORDSET(data->'lineup') 
        lineup(country jsonb
            ,player_id integer
            ,player_name text
            ,player_nickname text
        )
    )       
    '''
    print(f'      {conn.execute(str).rowcount} records loaded')
    conn.execute('CREATE INDEX idx_persons_name on persons(name)')

    # load referees from sb_matches into persons
    print('----- populating table persons with referees data...')
    str = '''
        INSERT INTO persons (id,name,country_id)
        (SELECT DISTINCT
            (data->'referee' ->>'id')::int
            ,data->'referee' ->>'name'
            ,(data->'referee' ->'country'->>'id')::int
        FROM sb_matches
        WHERE data->'referee' ->>'id' IS NOT NULL
          AND (data->'referee' ->>'id')::int NOT IN (SELECT id FROM persons)
        )
    '''
    print(f'      {conn.execute(str).rowcount} records loaded')

    # load managers from sb_matches into persons
    print('----- populating table persons with managers data...')
    str = '''
        INSERT INTO persons (id,name,nickname,dob,country_id)
        (SELECT DISTINCT
            manager.id,manager.name,manager.nickname,manager.dob,(manager.country->>'id')::int
        FROM sb_matches,
            JSONB_TO_RECORDSET(data->'home_team'->'managers') 
                manager(id        integer
                        ,name     text
                        ,nickname text
                        ,dob      date
                        ,country  jsonb
                )
        WHERE manager.id NOT IN (SELECT id FROM persons)
        UNION
        SELECT DISTINCT
            manager.id,manager.name,manager.nickname,manager.dob,(manager.country->>'id')::int
        FROM sb_matches,
            JSONB_TO_RECORDSET(data->'away_team'->'managers') 
                manager(id        integer
                        ,name     text
                        ,nickname text
                        ,dob      date
                        ,country  jsonb
                )
        WHERE manager.id NOT IN (SELECT id FROM persons)
        )
    '''
    print(f'      {conn.execute(str).rowcount} records loaded')

    # load teams from sb_matches into teams
    print('----- populating table teams...')
    str = '''
        INSERT INTO teams (team_id,team_name,gender,country_id)
        (SELECT DISTINCT
            (data->'home_team'->>'home_team_id')::int
            ,data->'home_team'->>'home_team_name'
            ,data->'home_team'->>'home_team_gender'
            ,(data->'home_team'->'country'->>'id')::int
        FROM sb_matches
        UNION
            SELECT DISTINCT
                (data->'away_team'->>'away_team_id')::int
                ,data->'away_team'->>'away_team_name'
                ,data->'away_team'->>'away_team_gender'
                ,(data->'away_team'->'country'->>'id')::int
            FROM sb_matches
        )
    '''
    print(f'      {conn.execute(str).rowcount} records loaded')
    conn.execute('CREATE INDEX idx_teams_name on teams(team_name)')

    # load_matches
    print('----- populating table matches...')
    str = '''
        INSERT INTO matches (match_id,match_date,kick_off,competition_id,season_id,competition_name, season_name,
                                home_team_id,away_team_id,home_team_group,away_team_group,home_score,away_score,
                                match_week,stadium_id,referee_id,competition_stage)
        (SELECT DISTINCT
            (data->>'match_id')::int
            ,(data->>'match_date')::date
            ,(data->>'kick_off')::time
            ,(data->'competition'->>'competition_id')::int
            ,(data->'season'->>'season_id')::int
            ,(data->'competition'->>'competition_name')
            ,(data->'season'->>'season_name')
            ,(data->'home_team'->>'home_team_id')::int
            ,(data->'away_team'->>'away_team_id')::int
            ,data->'home_team'->>'home_team_group'
            ,data->'away_team'->>'away_team_group'
            ,(data->>'home_score')::int
            ,(data->>'away_score')::int
            ,(data->>'match_week')::int
            ,(data->'stadium'->>'id')::int
            ,(data->'referee'->>'id')::int
            ,data->'competition_stage'->>'name'
        FROM sb_matches
        )
        '''
    print(f'      {conn.execute(str).rowcount} records loaded')
    conn.execute('CREATE INDEX idx_matches_competition_name on matches(competition_name)')
    conn.execute('CREATE INDEX idx_matches_season_name on matches(season_name)')
    
    # load data into players
    print('----- populating table players...')
    str = '''
        INSERT INTO players (player_id,team_id,match_id)
        (SELECT DISTINCT 
            lineup.player_id
            , (data->>'team_id')::integer
            , REPLACE(data->>'file_name','.json','')::integer
        FROM sb_lineups,
            JSONB_TO_RECORDSET(data->'lineup') lineup(player_id integer)
        )
    '''
    print(f'      {conn.execute(str).rowcount} records loaded')

    # load data into managers
    print('----- populating table managers...')
    str = '''
        INSERT INTO managers (manager_id,team_id,match_id)
        (SELECT DISTINCT
            manager.id
            , (data->'home_team'->>'home_team_id')::integer
            , (data->>'match_id')::integer 
        FROM sb_matches,
            JSONB_TO_RECORDSET(data->'home_team'->'managers') manager(id integer)
        WHERE data->'home_team'->'managers' IS NOT NULL

        UNION

        SELECT DISTINCT
            manager.id
            , (data->'away_team'->>'away_team_id')::integer
            , (data->>'match_id')::integer 
        FROM sb_matches,
            JSONB_TO_RECORDSET(data->'away_team'->'managers') manager(id integer)
        WHERE data->'away_team'->'managers' IS NOT NULL
        )
    '''
    print(f'      {conn.execute(str).rowcount} records loaded')

    # load event main data into events from sb_events
    print('----- populating table tmp_event_main...')
    str = '''
        INSERT INTO tmp_event_main (event_id,index,period,timestamp,minute,second,type,
            possession,possession_team_id,play_pattern,team_id,player_id,position,
            location,duration,under_pressure,off_camera,out,match_id)
        (SELECT
             (data->>'id')::uuid
            ,(data->>'index')::integer
            ,(data->>'period')::integer
            ,(data->>'timestamp')::time
            ,(data->>'minute')::integer
            ,(data->>'second')::integer
            ,data->'type'->>'name'
            ,(data->>'possession')::integer
            ,(data->'possession_team'->>'id')::integer
            ,data->'play_pattern'
            ,(data->'team'->>'id')::integer
            ,(data->'player'->>'id')::integer
            ,data->'position'->>'name'
            ,ARRAY[((data->'location')[0])::decimal,((data->'location')[1])::decimal]
            ,(data->>'duration')::decimal
            ,(data->>'under_pressure')::boolean
            ,(data->>'off_camera')::boolean
            ,(data->>'out')::boolean
            ,REPLACE(data->>'file_name','.json','')::integer     
        FROM sb_events
        );
    '''
    print(f'      {conn.execute(str).rowcount} records loaded')

    # load_event_data
    load_event_data(conn)
    
    # load event data into event_related from sb_events
    print('----- populating table event_related...')
    str = '''
        CREATE TABLE event_related AS
        SELECT (data->>'id')::uuid event_id
             , related_event::uuid
        FROM sb_events
           , JSONB_ARRAY_ELEMENTS_TEXT(data->'related_events') related_event
        WHERE data->>'related_events' IS NOT NULL
    '''
    print(f'      {conn.execute(str).rowcount} records loaded')
    # conn.execute('''ALTER TABLE event_related 
    #                   ADD PRIMARY KEY (event_id,related_event)
    #                 , ADD FOREIGN KEY (event_id) REFERENCES events
    #                 , ADD FOREIGN KEY (related_event) REFERENCES events(event_id)
    #              ''')
    # conn.execute('CREATE INDEX idx_event_related ON event_related(related_event')

    # load tactics data into event_tactics from sb_events
    print('----- populating table event_tactics...')
    str = '''
        CREATE TABLE event_tactics AS
        SELECT (data->>'id')::uuid event_id
             , data->'tactics'->>'formation' formation
             , (lineup.player->>'id')::integer player_id
             , lineup.jersey_number
        FROM sb_events
            , JSONB_TO_RECORDSET(data->'tactics'->'lineup') lineup(player jsonb, position jsonb, jersey_number integer)
        WHERE data->'tactics' IS NOT NULL
    '''
    print(f'      {conn.execute(str).rowcount} records loaded')
    # conn.execute('''ALTER TABLE event_tactics 
    #                 , ADD FOREIGN KEY (event_id) REFERENCES events
    #                 , ADD FOREIGN KEY (player_id) REFERENCES persons(id)
    #              ''')
    # conn.execute('CREATE INDEX idx_event_tactics_player_id ON event_tactics(player_id)')
    # conn.execute('CREATE INDEX idx_event_tactics_formation ON event_tactics(formation)')


    conn.commit()
    print("All data successfully loaded.")


#-----------------------------------------------------------------------------
# To extract data from sb_events and load to table events
#   conn =  connection to the database
#-----------------------------------------------------------------------------
def load_event_data(conn):

    db_fields = [
    ['50_50','_outcome','_counterpress']
    ,['bad_behaviour','_card']
    ,['ball_receipt','_outcome']
    ,['ball_recovery','_offensive','_recovery_failure']
    ,['block','_counterpress','_deflection','_offensive','_save_block']
    ,['carry','_end_location']
    ,['clearance','_aerial_won','_body_part']
    ,['dribble','_outcome','_nutmeg','_overrun','_no_touch']
    ,['dribbled_past','_counterpress']
    ,['duel','_counterpress','_type','_outcome']
    ,['foul_committed','_advantage','_counterpress','_offensive','_penalty','_card','_type']
    ,['foul_won','_advantage','_defensive','_penalty']
    ,['goalkeeper','_position','_technique','_body_part','_type','_outcome']
    ,['half_end','_early_video_end','_match_suspended']
    ,['half_start','_late_video_start']
    ,['injury_stoppage','_in_chain']
    ,['interception','_outcome']
    ,['miscontrol','_aerial_won']
    ,['pass','_recipient_id','_length','_angle','_height','_end_location','_assisted_shot_id','_backheel','_deflected','_miscommunication','_cross','_cut_back','_switch','_shot_assist','_goal_assist','_body_part','_type','_outcome','_technique']
    ,['player_off','_permanent']
    ,['pressure','_counterpress']
    ,['shot','_key_pass_id','_end_location','_aerial_won','_follows_dribble','_first_time','_freeze_frame','_open_goal','_statsbomb_xg','_deflected','_technique','_body_part','_type','_outcome']
    ,['substitution','_replacement_id','_outcome']
    ]

    sb_childs = [
    ["50_50","(data->'50_50'->'outcome'->>'name')","(data->'50_50'->>'counterpress')::boolean"]
    ,["bad_behaviour","(data->'bad_behaviour'->'card'->>'name')"]
    ,["ball_receipt","(data->'ball_receipt'->'outcome'->>'name')"]
    ,["ball_recovery","(data->'ball_recovery'->>'offensive')::boolean","(data->'ball_recovery'->>'recovery_failure')::boolean"]
    ,["block","(data->'block'->>'counterpress')::boolean","(data->'block'->>'deflection')::boolean","(data->'block'->>'offensive')::boolean","(data->'block'->>'save_block')::boolean"]
    ,["carry","(ARRAY[(data->'carry'->'end_location')[0],(data->'carry'->'end_location')[1]])::decimal []"]
    ,["clearance","(data->'clearance'->>'aerial_won')::boolean","(data->'clearance'->'body_part'->>'name')"]
    ,["dribble","(data->'dribble'->'outcome'->>'name')","(data->'dribble'->>'nutmeg')::boolean","(data->'dribble'->>'overrun')::boolean","(data->'dribble'->>'no_touch')::boolean"]
    ,["dribbled_past","(data->'dribbled_past'->>'counterpress')::boolean"]
    ,["duel","(data->'duel'->>'counterpress')::boolean","(data->'duel'->'type'->>'name')","(data->'duel'->'outcome'->>'name')"]
    ,["foul_committed","(data->'foul_committed'->>'advantage')::boolean","(data->'foul_committed'->>'counterpress')::boolean","(data->'foul_committed'->>'offensive')::boolean","(data->'foul_committed'->>'penalty')::boolean","(data->'foul_committed'->'card'->>'name')","(data->'foul_committed'->'type'->>'name')"]
    ,["foul_won","(data->'foul_won'->>'advantage')::boolean","(data->'foul_won'->>'defensive')::boolean","(data->'foul_won'->>'penalty')::boolean"]
    ,["goalkeeper","(data->'goalkeeper'->'position'->>'name')","(data->'goalkeeper'->'technique'->>'name')","(data->'goalkeeper'->'body_part'->>'name')","(data->'goalkeeper'->'type'->>'name')","(data->'goalkeeper'->'outcome'->>'name')"]
    ,["half_end","(data->'half_end'->>'early_video_end')::boolean","(data->'half_end'->>'match_suspended')::boolean"]
    ,["half_start","(data->'half_start'->>'late_video_start')::boolean"]
    ,["injury_stoppage","(data->'injury_stoppage'->>'in_chain')::boolean"]
    ,["interception","(data->'interception'->'outcome'->>'name')"]
    ,["miscontrol","(data->'miscontrol'->>'aerial_won')::boolean"]
    ,["pass","(data->'pass'->'recipient'->>'id')::integer","(data->'pass'->>'length')::decimal","(data->'pass'->>'angle')::decimal","(data->'pass'->'height'->>'name')",
    "(ARRAY[(data->'pass'->'end_location')[0],(data->'pass'->'end_location')[1]])::decimal []",
    "(data->'pass'->>'assisted_shot_id')::uuid","(data->'pass'->>'backheel')::boolean","(data->'pass'->>'deflected')::boolean","(data->'pass'->>'miscommunication')::boolean","(data->'pass'->>'cross')::boolean","(data->'pass'->>'cut_back')::boolean","(data->'pass'->>'switch')::boolean","(data->'pass'->>'shot_assist')::boolean","(data->'pass'->>'goal_assist')::boolean","(data->'pass'->'body_part'->>'name')","(data->'pass'->'type'->>'name')","(data->'pass'->'outcome'->>'name')","(data->'pass'->'technique'->>'name')"]
    ,["player_off","(data->'player_off'->>'permanent')::boolean"]
    ,["pressure","(data->'pressure'->>'counterpress')::boolean"]
    ,["shot","(data->'shot'->>'key_pass_id')::uuid",
        '''CASE (data->'shot'->'end_location')[2]
                WHEN  NULL THEN 
                    ARRAY[(data->'shot'->'end_location')[0],(data->'shot'->'end_location')[1]]
                ELSE
                    ARRAY[(data->'shot'->'end_location')[0],(data->'shot'->'end_location')[1],(data->'shot'->'end_location')[2]]
            END :: decimal[] ''',
        "(data->'shot'->>'aerial_won')::boolean","(data->'shot'->>'follows_dribble')::boolean","(data->'shot'->>'first_time')::boolean","(data->'shot'->'freeze_frame')","(data->'shot'->>'open_goal')::boolean","(data->'shot'->>'statsbomb_xg')::decimal","(data->'shot'->>'deflected')::boolean","(data->'shot'->'technique'->>'name')","(data->'shot'->'body_part'->>'name')","(data->'shot'->'type'->>'name')","(data->'shot'->'outcome'->>'name')"]
    ,["substitution","(data->'substitution'->'replacement'->>'id')::integer","(data->'substitution'->'outcome'->>'name')"]
    ]

    str_events = '''
        CREATE TABLE events AS 
        SELECT 
            tmp_event_main.event_id
            , index
            , period
            , timestamp
            , minute
            , second
            , type
            , possession
            , possession_team_id
            , play_pattern
            , team_id
            , player_id
            , position
            , location
            , duration
            , under_pressure
            , off_camera
            , out
            , match_id
            , _advantage
            , _aerial_won
            , _angle
            , _assisted_shot_id
            , _backheel
            , _body_part
            , _card
            , _counterpress
            , _cross
            , _cut_back
            , _defensive
            , _deflected
            , _deflection
            , _early_video_end
            , _end_location
            , _first_time
            , _follows_dribble
            , _freeze_frame
            , _goal_assist
            , _height
            , _in_chain
            , _key_pass_id
            , _late_video_start
            , _length
            , _match_suspended
            , _miscommunication
            , _no_touch
            , _nutmeg
            , _offensive
            , _open_goal
            , _outcome
            , _overrun
            , _penalty
            , _permanent
            , _position
            , _recipient_id
            , _recovery_failure
            , _replacement_id
            , _save_block
            , _shot_assist
            , _statsbomb_xg
            , _switch
            , _technique
            , _type
        FROM tmp_event_main
        NATURAL LEFT JOIN tmp_event_data
    '''

    # populate table event_data_wide
    for i in range(len(sb_childs)):
        print(f'----- loading data for {sb_childs[i][0]}...')

        si = "INSERT INTO tmp_event_data (event_id"
        sq = "SELECT (data->>'id')::uuid" 
        # print(s)
        for j in range(len(sb_childs[i])-1):
            sq = sq + "," + sb_childs[i][j+1] 
            si = si + "," + db_fields[i][j+1]
            
        sq = "(" + sq + f" FROM sb_events WHERE data->'{sb_childs[i][0]}' IS NOT NULL)"  
        si = si + ") "
        str = si + sq 

        print(f'      {conn.execute(str).rowcount} records loaded')

    # combine tmp_event_main and tmp_event_data into table events for analysis purposes
    print('----- populating table events...')
    print(f'      {conn.execute(str_events).rowcount} records loaded')

    print('----- building primary and foreign keys...')
    conn.execute(''' 
                ALTER TABLE events ADD PRIMARY KEY (event_id)
                , ADD FOREIGN KEY (match_id) REFERENCES matches
                , ADD FOREIGN KEY (team_id) REFERENCES teams
                , ADD FOREIGN KEY (possession_team_id) REFERENCES teams
                , ADD FOREIGN KEY (player_id) REFERENCES persons
                , ADD FOREIGN KEY (_recipient_id) REFERENCES persons
                , ADD FOREIGN KEY (_replacement_id) REFERENCES persons
                '''
    )

    print('----- creating index on events...')
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_player ON events(player_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_team ON events(team_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_recipient ON events(_recipient_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_first_time ON events(_first_time)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_technique ON events(_technique)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_outcome ON events(_outcome)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_end_location ON events(_end_location)")

   
#-------------------------------------------
# main
#-------------------------------------------
def main():
    # Define your PostgreSQL database connection details
    try:
        conn = psycopg.connect(
            # dbname="statsbomb",
            dbname="3005",
            user="postgres",
            password="8023",
            host="localhost",
            port=5432
        )

        # load_event_data(conn)

        print('Import json data into sb tables')
        import_sbdata(conn)

        print('Create table schema to hold data from the sb tables')
        create_db_schema(conn)

        print('Parse json data and populate the database')
        parse_sbdata(conn)

    except Exception as e:
        # print(f"Error: {e}")
        print(traceback.format_exc())

    finally:
        conn.commit()
        conn.close()
#-----------------------------------------------------

if __name__ == '__main__':
    main()
