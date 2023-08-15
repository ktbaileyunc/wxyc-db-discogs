import discogs_client
import time
import pandas as pd
import re
from pathlib import Path


YOUR_EMAIL = "email@email.com"
DISCOGS_API_TOKEN = "here"
NO_DATA = None
last_index: int = 0


# setting up discogs API
d = discogs_client.Client(f'WXYC Station Report Testing/0.1 +{YOUR_EMAIL}',user_token=DISCOGS_API_TOKEN)

# csv --> pandas db
filename = Path("wxyc_db/wxyc_db.csv")
specific_rows = range(56200, 56220)
df = pd.read_csv(filename, skiprows = lambda x: (x not in specific_rows and x != 0))
df = df.rename(columns={"Genre":"StationGenre"})
df.to_csv("wxyc_with_discogs2.csv")


# make columns with default value (no data)
df['DiscogsID'] = NO_DATA
df['DiscogsURL'] = NO_DATA
df['Checked'] = NO_DATA


print(f"{len(df)} items in specified portion of wxyc library. starting discogs now...")

# time functions track how long it's taking, which can be helpful (or interesting)
start_time = time.time()


# row by row to get data from discogs api
for index,rows in df.iterrows():
    time.sleep(1)

    # some exception handling - will probably eventually move this to a different file to run a second time, on whatever didn't match
    df.at[index, "Checked"] = "yes"
    last_index += 1

    entry_title = df.at[index, "Title"]
    entry_artist = df.at[index,"Artist"]
    entry_format = df.at[index,"Format"]

    if "[" in entry_title:
        df.at[index, "Title"] = re.sub("\[.*?\]","",entry_title)

    if entry_format == "vinyl - LP x 2":
        df.at[index,"Format"] = "vinyl"

    # SOUNDTRACKS

    # v/a Comps

    # 'EP'

    try:
        # first search release section
        results = d.search(title=entry_title,artist=entry_artist,type="release")
        first_result = results.page(1)[0]
        df.at[index, "DiscogsID"] = first_result.id
        df.at[index, "DiscogsURL"] = f"https://www.discogs.com/release/{first_result.id}"
    except (IndexError, TypeError):
        try:
            # now try master section
            time.sleep(1)
            results = d.search(title=entry_title,artist=entry_artist,type="master")
            first_result = results.page(1)[0]
            df.at[index, "DiscogsID"] = first_result.id
            df.at[index, "DiscogsURL"] = f"https://www.discogs.com/master/{first_result.id}"
        except (IndexError, TypeError):
            # this is where we give up, folks
            continue
    
    try:
        # ok cool so each result is a discogs object
        # we can get genres from it
        genre = first_result.genres
        for i in range(len(genre)):
            # adds genre to appropriate column
            df.at[index, f"DiscogsGenre{i+1}"] = genre[i]
    except (IndexError, TypeError):
        continue
    
    try: 
        # same thing for styles (i.e. sub-genres)
        style = first_result.styles
        for i in range(len(style)):
            df.at[index, f"DiscogsStyle{i+1}"] = style[i]
    except (IndexError, TypeError):
        continue
    
    # time limit (for now)
    #if time.time() - start_time > 60:
    #    break

end_time = time.time()
print(f"ok, that took {round(end_time - start_time, 1)}s for {last_index} releases, which is {round((end_time - start_time)/last_index, 3)}s on average... long live radio")

# save to csv, will go to output folder
df.to_csv("output/wxyc_with_discogs5.csv", index=False)
