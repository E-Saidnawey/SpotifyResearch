# SpotifyResearch

Welcome to my spotify research! This github page shows the process of me creating a dashboard for my spotify data. If you want to do this with your own data, I'll detail the steps below.

1. Request extended spotify data, as detailed here: https://support.stats.fm/docs/import/spotify-import/
  - note: you don't have to upload it to stats.fm, only go up to step 5.

2. Create a .env file
  - Your .env file should have a layout such as

    `INPUT_JSON_FOLDER="YOUR EXTRACTED FOLDER OF SPOTIFY JSON FILES"`
    `CLEAN_JSON_DATA="FULL FILE OF OUTPUT JSON NAME. ex: /home/eric-saidnawey/ToolRepository/SpotifyResearch/actual_clean_data.json"` 
   
