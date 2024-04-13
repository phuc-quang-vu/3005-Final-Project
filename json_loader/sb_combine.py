'''
----------------------------------------------------------------------------------------
This code used to combine statsbomb json files and create 3 following files

1. sb_matches.json : contains json data for all La Liga and Premier League matches
2. sb_lineups.json : contains lineup data for matches under La Liga season 2018/2019,
                     2019/2020 and 2020/2021 and Premier League season 2003/2004
3. sb_events.json  : contains event data for matches under La Liga season 2018/2019, 
                     2019/2020 and 2020/2021 and Premier League season 2003/2004

These files will be placed in folder statsbomb

The stabsbomb folder with all its file and subfolders (competitions.json, events, 
lineups, matches...) downloaded from GitHub should be placed under folder json_loader 
together with the script (sb_combine_json.py and sb_parse_data.py)
----------------------------------------------------------------------------------------
'''

import os
import json
from datetime import datetime


# -------------------------------------------------------------------------
# to build a list with (file name, file path) under the selected direcory 
# and its sub directories
# -------------------------------------------------------------------------
def get_files(file_path, dir_list = None, file_list = None):
    # file_path must end with '/'
    if file_path[-1] != '/': file_path = file_path + '/'

    # get files under file_path
    files = [(f,file_path) for f in os.listdir(file_path) if os.path.isfile(file_path+f)]
    if file_list is not None: files = [(f,p) for (f,p) in files if f in file_list]

    # get subdirectories under file_path
    dirs = [d for d in os.listdir(file_path) if os.path.isdir(file_path+d)]
    if dir_list is not None: dirs = [d for d in dirs if (file_path+d) in dir_list]

    # get files in subdirectories    
    for d in dirs:
        fs = [(f,file_path + d +'/') for f in os.listdir(file_path + d) if os.path.isfile(file_path+d+'/'+f)]
        if file_list is not None: fs = [(f,p) for (f,p) in fs if f in file_list] 
        files = files + fs

    return files



# -------------------------------------------------------------------------
# to combine json files listed in the list 'files' into a single data file
# indicated by <combined_file_path>
# -------------------------------------------------------------------------
def combine_files(combined_file_path, files):
    df = []

    for i in range(len(files)):
        print('processing', files[i][0],'...')

        with open(files[i][1]+files[i][0], mode = 'r',encoding = 'utf-8') as f:
            data = json.load(f)
            for d in data:
                d['file_name'] = files[i][0]
        df = df + data

    print('writing to',combined_file_path,'...')

    with open(combined_file_path, mode='w', encoding='utf-8') as f:
        json.dump(df,f,indent=2)

    print('...file saved')

#---------------------------------------------
# main program
#---------------------------------------------  
def main():
    # get files for La Liga (folder '11') and Premier League (folder '2') 
    competition_list=['statsbomb/matches/11','statsbomb/matches/2']

    fs = get_files('statsbomb/matches/', competition_list)
    combined_file_path = "statsbomb/sb_matches.json"
    combine_files(combined_file_path,fs)  

    # get match_id of La Liga season 2018/2019, 2019/2020 and 2020/2021 
    # and Premier League season 2003/2004 matches
    # to load only relevant data for lineups and events
    with open('statsbomb/sb_matches.json', encoding='utf-8') as f:
        data = json.load(f)
        df = [str(x['match_id'])+'.json' for x in data
                if (x['competition']['competition_name']=='La Liga' and x['season']['season_name'] in ['2018/2019','2019/2020','2020/2021']) 
                or (x['competition']['competition_name']=='Premier League' and x['season']['season_name']=='2003/2004')
             ] 

    fs = get_files('statsbomb/lineups/', file_list = df)
    combined_file_path = "statsbomb/sb_lineups.json"
    combine_files(combined_file_path,fs)  

    fs = get_files('statsbomb/events/', file_list = df)
    combined_file_path = "statsbomb/sb_events.json"
    combine_files(combined_file_path,fs)  


# main program
#-----------------------------------------
if __name__ == '__main__':
    main()
