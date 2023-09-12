import discogs_client
import time
import pandas as pd
import re
from pathlib import Path


# important notes: the index (i.e. row number in the csv) is NOT the same as the card catalog id number! so don't worry if those don't line up
# your wifi MUST be stable or else the program will crash

YOUR_EMAIL = "ktbailey@unc.edu"
DISCOGS_API_TOKEN = "WYGUVmaQhjlcAOqIVTTzImqgtfLxJRNgQSwqooLK"
NO_DATA = None
last_index: int = 0


# setting up discogs API
d = discogs_client.Client(f'WXYC Station Report Testing/0.1 +{YOUR_EMAIL}',user_token=DISCOGS_API_TOKEN)

# csv --> pandas db
filename = Path("wxyc_db/wxyc_db.csv")

# prompt user to input desired range of rows to process
start_idx = int(input("Enter the starting row index: "))
end_idx = int(input("Enter the ending row index: "))
specific_rows = range(start_idx, end_idx+1)
total_rows = end_idx - start_idx

df = pd.read_csv(filename, skiprows = lambda x: (x not in specific_rows and x != 0))

# make columns with default value (no data)
df['DiscogsID'] = NO_DATA
df['DiscogsURL'] = NO_DATA
df['Checked'] = NO_DATA
for i in range(1,6):
    df[f'DiscogsGenre{i}'] = NO_DATA
for j in range(1,6):
    df[f'DiscogsStyle{j}'] = NO_DATA


print(f"{len(df)} items in specified portion of wxyc library. starting discogs now...")

# time functions track how long it's taking, which can be helpful (or interesting)
start_time = time.time()


# row by row to get data from discogs api
for index,rows in df.iterrows():
    time.sleep(1)
    if last_index % (round(total_rows/10)) == 0 and last_index != 0:
        print(f"{(last_index/round(total_rows/10))*10}% done")
        output_filename = f"output/wxyc_with_discogs_{start_idx}_to_{end_idx}_BACKUP.csv"
        df.to_csv(output_filename, index=False)

    # some exception handling - will probably eventually move this to a different file to run a second time, on whatever didn't match
    df.at[index, "Checked"] = "yes"
    last_index += 1

    entry_title = df.at[index, "Title"]
    entry_artist = df.at[index,"Artist"]
    entry_format = df.at[index,"Format"]
    found_type = "master"

    # print(f"starting search for {entry_title}")

    if "[" in entry_title:
        entry_title = re.sub("\[.*?\]","",entry_title)

    if df.at[index, "StationGenre"] == "Soundtracks":
        entry_artist = ""
        entry_title += "Soundtrack"

    if "Various Artists" in entry_artist:
        entry_artist = "Various"
    
    try:
        results = d.search(entry_title,artist=entry_artist,type="master")
    except ConnectionError:
        print(f"Connection error at Release ID {df.at[index, 'ReleaseId']}, saving backup")
        output_filename = f"output/wxyc_with_discogs_{start_idx}_to_{end_idx}_BACKUP.csv"
        df.to_csv(output_filename, index=False)
    # print(f"{entry_title} has {len(results)} for master")
    if len(results) == 0:
        time.sleep(1)
        try:
            results = d.search(entry_title,artist=entry_artist,type="release")
        except ConnectionError:
            print(f"Connection error at Release ID {df.at[index, 'ReleaseId']}, saving backup")
            output_filename = f"output/wxyc_with_discogs_{start_idx}_to_{end_idx}_BACKUP.csv"
            df.to_csv(output_filename, index=False)
        # print(f"{entry_title} has {len(results)} for release")
        found_type = "release"
        if len(results) == 0 and entry_artist != "Various" and entry_artist != "":
            time.sleep(1)
            try:
                results = d.search(artist=entry_artist,type="master")
            except:
                print(f"Connection error at Release ID {df.at[index, 'ReleaseId']}, saving backup")
                output_filename = f"output/wxyc_with_discogs_{start_idx}_to_{end_idx}_BACKUP.csv"
                df.to_csv(output_filename, index=False)
            df.at[index, "Checked"] = "yes but artist"
            found_type = "master"

    try:
        first_result = results.page(1)[0]
    except IndexError:
        continue
    
        # look at releases
    df.at[index, "DiscogsID"] = first_result.id
    df.at[index, "DiscogsURL"] = f"https://www.discogs.com/{found_type}/{first_result.id}"
    # print(f"we found one for {index}\n")

    # print(first_result)
    try:
        # ok cool so each result is a discogs object
        # we can get genres from it
        # print(first_result.genres)
        genre = first_result.genres
        for i in range(len(genre)):
            # adds genre to appropriate column
            if i < 5:
                df.at[index, f"DiscogsGenre{i+1}"] = genre[i]

    except (IndexError, TypeError):
        continue
    
    try: 
        # same thing for styles (i.e. sub-genres)
        # print(f"{first_result.styles}\n\n")
        style = first_result.styles
        for j in range(len(style)):
            if j < 5:
                df.at[index, f"DiscogsStyle{j+1}"] = style[j]
    except (IndexError, TypeError):
        pass
    
    # time limit (for now)
    #if time.time() - start_time > 60:
    #    break

end_time = time.time()
print(f"ok, that took {round(end_time - start_time, 1)}s for {last_index} releases, which is {round((end_time - start_time)/last_index, 3)}s on average... long live radio")

# rename + save to csv, will go to output folder
output_filename = f"output/wxyc_with_discogs_{start_idx}_to_{end_idx}.csv"
df.to_csv(output_filename, index=False)