import discogs_client
import time
import pandas as pd
import re
from pathlib import Path

start_index: int
end_index: int
last_index: int = 0

def main() -> None:
    # Set up Discogs API client
    YOUR_EMAIL = "ktbailey@unc.edu"
    DISCOGS_API_TOKEN = "WYGUVmaQhjlcAOqIVTTzImqgtfLxJRNgQSwqooLK"
    d = discogs_client.Client(f'WXYC Station Report Testing/0.1 +{YOUR_EMAIL}',user_token=DISCOGS_API_TOKEN)
    
    global last_index

    # Create pandas dataframe from csv of releases
    df = createBaseDf(Path("wxyc_db/wxyc_db.csv"))

    # Time functions track how long it's taking, which can be helpful (or just interesting)
    start_time = time.time()
    print(f"{len(df)} items in specified portion of WXYC library. starting Discogs script now...")

    # Search Discogs row-by-row (i.e. release-by-release)
    for index, rows in df.iterrows():
        time.sleep(1)
        checkProgress(df)

        last_index += 1
        df.at[index, "Checked"] = "yes"

        # Extract release details from dataframe
        entry_title = cleanTitle(df, index, str(df.at[index, "Title"]))
        entry_artist = cleanTitle(df, index, str(df.at[index, "Artist"]))
        search_type: str
        
        # Initial Discogs search
        try:
            search_type = "master" # lets us construct a Discogs URL later
            results = d.search(entry_title, artist=entry_artist, type=search_type)
        except ConnectionError:
            connectionError(df)

        # The above works most of the time, but if not, these are some alternate seaches

        # Search Discogs for "releases" instead of "master releases"
        # Usually, we prefer masters because they have better genre data, but we'll take a regular release if it's the only one available
        if len(results) == 0:
            time.sleep(1)
            try:
                search_type = "release"
                results = d.search(entry_title, artist=entry_artist, type=search_type)
            except ConnectionError:
                connectionError(df)

        # Our last resort is searching for anything by the artist. Chances are, the genre will be similar
        if len(results) == 0 and entry_artist != "Various" and entry_artist != "":
            time.sleep(1)
            try:
                df.at[index, "Checked"] = "yes - artist only"
                search_type = "master"
                results = d.search(artist=entry_artist, type=search_type)
            except:
                connectionError(df)

        # Still nothing? (This is rare) We can go to the next thing
        try:
            first_result = results.page(1)[0]
        except IndexError:
            df.at[index, "Checked"] = "yes - not found"
            continue

        first_result = results.page(1)[0]
        extractDiscogsInfo(df, index, first_result, search_type)
    
    # Last print statement. Includes some stats about how long it took
    print(f"done! that took {round(time.time() - start_time, 1)}s for {last_index} releases, which is {round((time.time() - start_time)/last_index, 3)}s on average.")

    # Save final csv
    saveCSV(df)

    print("final csv saved in output folder. long live radio!")


def createBaseDf(filename):
    global start_index, end_index
    start_index = int(input("Enter the starting row index: "))
    end_index = int(input("Enter the ending row index: "))
    specific_rows = range(start_index, end_index + 1)
    
    df = pd.read_csv(filename, skiprows = lambda x: (x not in specific_rows and x != 0))
   
    # make columns with default values
    df['DiscogsID'] = None
    df['DiscogsURL'] = None
    df['Checked'] = None
    for i in range(1,6):
        df[f'DiscogsGenre{i}'] = None
    for j in range(1,6):
        df[f'DiscogsStyle{j}'] = None

    return df


def saveCSV(df, backup=False, error=False) -> None:
    global start_index, end_index, last_index
    # Save df as CSV, different name if it's a standard backup (these will overwrite themselves, the final save should be unique)
    if backup is True:
        if error is False:
            output_filename = f"output/wxyc_with_discogs_{start_index}_to_{end_index}_RoutineBackup.csv"
        else:
            output_filename = f"output/wxyc_with_discogs_{start_index}_to_{start_index + last_index}_LatestBackup.csv"
    else:
        output_filename = f"output/wxyc_with_discogs_{start_index}_to_{end_index}.csv"
    df.to_csv(output_filename, index=False)


def checkProgress(df) -> None:
    global start_index, end_index, last_index
    if last_index % (round((end_index - start_index)/10)) == 0 and last_index != 0:
        # Print progress updates after each 10% is complete
        print(f"{(last_index/round((end_index - start_index)/10))*10}% done. saving backup just in case.")
        saveCSV(df, backup=True)


def connectionError(df) -> None:
    print(f"connection (wifi) error at {start_index + last_index-1}. saving backup.")
    saveCSV(df, backup=True, error=True)


def cleanTitle(df, index, title: str) -> str:
    # Remove brackets from title
    if "[" in title:
        title = re.sub("\[.*?\]","",title)

    # Add "Soundtrack" to soundtrack titles to help search
    if df.at[index, "StationGenre"] == "Soundtracks":
        title += "Soundtrack"
    
    return title


def cleanArtist(df, index, artist: str) -> str:
    # Change v/a releases to Various in line with Discogs norm
    if "Various Artists" in artist:
        artist = "Various"
    
    # Remove XYC soundtrack designation from artist field
    if df.at[index, "StationGenre"] == "Soundtracks":
        artist = ""
    
    return artist

def extractDiscogsInfo(df, index, first_result, search_type) -> None:
    # Add Discogs ID to results
    df.at[index, "DiscogsID"] = first_result.id
    # Add Discogs URL to results so we can refer back and check our results
    df.at[index, "DiscogsURL"] = f"https://www.discogs.com/{search_type}/{first_result.id}"

    # Getting up to 5 genres
    try:
        genre = first_result.genres
        for i in range(len(genre)):
            # adds genre to appropriate column
            if i < 5:
                df.at[index, f"DiscogsGenre{i+1}"] = genre[i]

    except (IndexError, TypeError):
        return 

    # Getting up to 5 styles (i.e., sub-genres)
    try: 
        style = first_result.styles
        for j in range(len(style)):
            if j < 5:
                df.at[index, f"DiscogsStyle{j+1}"] = style[j]
    except (IndexError, TypeError):
        return 
    
    return
    

if __name__ == "__main__":
    main()