import sys
import tqdm
import time
import math
import logging
import requests
import argparse
import pandas as pd

def set_up_log():
    """
    set up logging
    log will print to file and console
    """
    logging.basicConfig(filename='log',
                        format='%(asctime)s [%(levelname)s] %(message)s',
                        level=logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

def parse_arguements() -> argparse.ArgumentParser:
    """
    parse arguements
    arguements will print to log file
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="input csv")
    args = parser.parse_args()

    # print arguements to console and log
    for arg in vars(args):
        logging.info(f"{arg:<15} =  {getattr(args, arg)}")
    return args

def read_input_file(input: str) -> pd.DataFrame:
    """
    read csv dataframe
    """
    return pd.read_csv(input)

def get_response(url: str):
    """
    make api call
    use requests to pull city and temperature data
    """
    response = requests.get(url)
    return response.json()

def get_city_and_temperature_from_postal_code(df: pd.DataFrame) -> pd.DataFrame:
    """
    add city and temp to input dataframe
    """
    # add placeholders colums to dataframe
    df["city"] = ["NaN"] * len(df.index)
    df["temperature"] = ["NaN"] * len(df.index)

    # loop over each data and process based on postal_code
    for index, row in tqdm.tqdm(df.iterrows(), total=len(df.index)):
        
        # make sure postal_code is valid. do not process if not valid
        postal_code = row["postal_code"]
        if len(str(postal_code)) != 5: continue

        # address which we pass to weather api
        url = f"https://api.openweathermap.org/data/2.5/weather?zip={postal_code},us&appid=eecda87a4e6bb67b0176b7ea27e17f29&units=imperial"

        # keep trying an api call until successful b/c 60 calls/minute
        found = False
        while not found:
            try:
                response = get_response(url)
                found = True
            except requests.exceptions.SSLError as e:
                # we can only have 60 api calls/minute
                time.sleep(1)
            
        # update city and temperature in dataframe
        df.at[index, "city"] = response["name"]
        df.at[index, "temperature"] = response["main"]["temp"]
        
    return df

def save_dataframe(df: pd.DataFrame, name: str):
    """
    saves dataframe to csv
    """
    df.to_csv(name, index=False)

# this function works but uses a different logic from the others
#def get_cities_by_gender(df: pd.DataFrame) -> pd.DataFrame:
#    """
#    returns a dataframe of city, gender, num_users
#    """
#    cities_by_gender = pd.DataFrame({"city": [],
#                                     "gender": [],
#                                     "num_users": []})
#
#    # loop over input dataframe
#    for index, row in tqdm.tqdm(df.iterrows(), total=len(df.index)):
#        
#        # bad way to fix a problem (we initialized values as NaN earlier)
#        if isinstance(row["gender"], float): row["gender"] = "NaN"
#        
#        # check if city already exist in our new dataframe
#        if row["city"] in set(cities_by_gender["city"]):
#
#            # create a subset of city dataframe
#            subset = cities_by_gender[cities_by_gender["city"] == row["city"]]
#            
#            # check if that specific gender is in our subset
#            if row["gender"] in set(subset["gender"]):
#
#                # get index of same gender
#                i = subset[subset["gender"] == row["gender"]].index[0]
#                
#                # update number of people
#                cities_by_gender.loc[i:i, "num_users"] += 1
#            else:
#                # create a new row in dataframe and fill with city, gender, num_users
#                cities_by_gender.loc[len(cities_by_gender.index)] = [row["city"], row["gender"], 1]
#        else:
#            # create a new row in dataframe and fill with city, gender, num_users
#            cities_by_gender.loc[len(cities_by_gender.index)] = [row["city"], row["gender"], 1]
#
#    return cities_by_gender
def get_cities_by_gender(df: pd.DataFrame) -> pd.DataFrame:
    """
    returns a dataframe of city, gender, num_users
    """
    cities_by_gender = pd.DataFrame({"city": [],
                                     "gender": [],
                                     "num_users": []})
    for city in tqdm.tqdm(df.city.unique(), total=len(df.city.unique())):

        # create subset of cities from input dataframe
        city_subset = df[df["city"] == city]
        num_users = len(city_subset)
        
        # create a subset of subset of women from those cities
        num_female = len(city_subset[city_subset["gender"] == "female"])
        if num_female:
            i = len(cities_by_gender)
            cities_by_gender.loc[i] = [city, "female", num_female]

        # create a subset of subset of men from those cities
        num_male = len(city_subset[city_subset["gender"] == "male"])
        if num_male:
            i = len(cities_by_gender)
            cities_by_gender.loc[i] = [city, "male", num_male]

        # create a subset of subset of non_binary from those cities
        num_non_binary = len(city_subset[city_subset["gender"] == "non_binary"])
        if num_non_binary:
            i = len(cities_by_gender)
            cities_by_gender.loc[i] = [city, "non_binary", num_non_binary]

        # count number of blank rows
        num_blank = city_subset['gender'].isna().sum()
        if num_blank:
            i = len(cities_by_gender)
            cities_by_gender.loc[i] = [city, "NaN", num_blank]
            
    return cities_by_gender


def get_cities_by_gender_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    returns dataframe city, female_percent, male_percent, non_binary_percent, blank_percent
    """
    cities_by_gender_distribution = pd.DataFrame({"city": [],
                                                  "female_percent": [],
                                                  "male_percent": [],
                                                  "non_binary_percent": [],
                                                  "blank_percent": []})

    # loop over cities
    for city in tqdm.tqdm(df.city.unique(), total=len(df.city.unique())):

        # create subset of citys from input dataframe
        subset = df[df["city"] == city]

        # total users for a specific city
        total = subset["num_users"].sum()

        # initialize row
        i = len(cities_by_gender_distribution.index)
        cities_by_gender_distribution.loc[i] = [city, 0, 0, 0, 0]

        # loop over subset and update the gender_percent
        for index, row in subset.iterrows():
            if row["gender"] == "female":
                cities_by_gender_distribution.loc[i:i, "female_percent"] = row["num_users"] / total
            elif row["gender"] == "male":
                cities_by_gender_distribution.loc[i:i, "male_percent"] = row["num_users"] / total
            elif row["gender"] == "non_binary":
                cities_by_gender_distribution.loc[i:i, "non_binary_percent"] = row["num_users"] / total
            else:
                cities_by_gender_distribution.loc[i:i, "blank_percent"] = row["num_users"] / total

    return cities_by_gender_distribution

def get_cities_by_avg_temp(df: pd.DataFrame) -> pd.DataFrame:
    """
    returns dataframe city, avg_temp
    """
    cities_by_avg_temp = pd.DataFrame({"city": [],
                                       "avg_temp": []})

    # loop over cities
    for city in tqdm.tqdm(df.city.unique(), total=len(df.city.unique())):
        
        # create subset of citys from input dataframe
        subset = df[df["city"] == city]

        # initialize and populate row
        i = len(cities_by_avg_temp.index)
        cities_by_avg_temp.loc[i] = [city, subset["temperature"].mean()]

    return cities_by_avg_temp

def get_top_10_cities_by_temp(df: pd.DataFrame) -> pd.DataFrame:
    """
    returns dataframe city, avg_temp, female_percent
    """
    top_10_cities_by_temp = pd.DataFrame({"city": [],
                                          "avg_temp": [],
                                          "female_percent": []})

    # loop over cities
    for city in tqdm.tqdm(df.city.unique(), total=len(df.city.unique())):

        # create subset of cities from input dataframe
        city_subset = df[df["city"] == city]
        num_users = len(city_subset)
        
        # create a subset of subset of women from those cities
        female_subset = city_subset[city_subset["gender"] == "female"]
        num_female = len(female_subset)

        # calc female percent (avoid div by 0)
        female_percent = 0
        if num_users > 0:
            female_percent = num_female/num_users 
        
        # initialize and populate row
        i = len(top_10_cities_by_temp.index)
        top_10_cities_by_temp.loc[i] = [city, city_subset["temperature"].mean(), female_percent]

    # query parameters
    top_10_cities_by_temp = top_10_cities_by_temp.sort_values(by="avg_temp", ascending=False)
    top_10_cities_by_temp = top_10_cities_by_temp[top_10_cities_by_temp["female_percent"] > 0.5]
    top_10_cities_by_temp = top_10_cities_by_temp[:10]

    return top_10_cities_by_temp

if __name__ == "__main__":
    set_up_log()
    args = parse_arguements()
    
    df = read_input_file(args.input)
    
    output = get_city_and_temperature_from_postal_code(df)
    save_dataframe(output, "output.csv")

    cities_by_gender = get_cities_by_gender(output)
    save_dataframe(cities_by_gender, "cities_by_gender.csv")

    cities_by_gender_distribution = get_cities_by_gender_distribution(cities_by_gender)
    save_dataframe(cities_by_gender_distribution, "cities_by_gender_distribution.csv")

    cities_by_avg_temp = get_cities_by_avg_temp(output)
    save_dataframe(cities_by_avg_temp, "cities_by_avg_temp.csv")

    top_10_cities_by_temp = get_top_10_cities_by_temp(output)
    save_dataframe(top_10_cities_by_temp, "top_10_cities_by_temp.csv")
