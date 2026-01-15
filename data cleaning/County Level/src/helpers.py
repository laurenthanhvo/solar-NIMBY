import pandas as pd
from src.CONSTANTS import get_file_path


FIPS_DF = pd.read_csv(get_file_path('FIPS'), dtype=str)
EIA_FIPS = pd.read_csv(get_file_path('EIA_Electric'), dtype={'utility_id_eia': str, 'county_id_fips': str})[['county_id_fips', 'utility_id_eia']]

def to_int(df):
    # strip all ',' from string numbers
    df = df.apply(lambda x: x.replace(",", "") if type(x) == str else x)
    return df.astype(float)

def merged_normalized_data(wind, gdp, solar, BB):
    important_cols = ["State", "County Name", "Wind Capacity Intensity (MW / 1000 sq mile)", "Wind Project Intensity (Projects / 1000 sq mile)", "Wind Avg Capacity Intensity (MW / 1000 sq mile)", "GDP_2017", "GDP_2018", "GDP_2019", "GDP_2020", "GDP_2021", "GDP_2022"]

    merged = wind.merge(gdp, on=["State", "County Name"], how='outer')

    merged = merged[important_cols]
    
    merged = BB.merge(merged, on=["State", "County Name"], how='outer')
    
    if type(solar) == dict:
        for key in solar.keys():
            merged = merged.merge(solar[key], on=["State", "County Name"], how='outer')
    else:
        merged = merged.merge(solar, on=["State", "County Name"], how='outer')
        

    return merged

def FIPS_getter():
    return FIPS_DF
    
def EIA_FIPS_getter():
    return EIA_FIPS

def get_state_abbr():
    return {
        "AL": "Alabama",
        "AK": "Alaska",
        "AZ": "Arizona",
        "AR": "Arkansas",
        "CA": "California",
        "CO": "Colorado",
        "CT": "Connecticut",
        "DE": "Delaware",
        "FL": "Florida",
        "GA": "Georgia",
        "HI": "Hawaii",
        "ID": "Idaho",
        "IL": "Illinois",
        "IN": "Indiana",
        "IA": "Iowa",
        "KS": "Kansas",
        "KY": "Kentucky",
        "LA": "Louisiana",
        "ME": "Maine",
        "MD": "Maryland",
        "MA": "Massachusetts",
        "MI": "Michigan",
        "MN": "Minnesota",
        "MS": "Mississippi",
        "MO": "Missouri",
        "MT": "Montana",
        "NE": "Nebraska",
        "NV": "Nevada",
        "NH": "New Hampshire",
        "NJ": "New Jersey",
        "NM": "New Mexico",
        "NY": "New York",
        "NC": "North Carolina",
        "ND": "North Dakota",
        "OH": "Ohio",
        "OK": "Oklahoma",
        "OR": "Oregon",
        "PA": "Pennsylvania",
        "RI": "Rhode Island",
        "SC": "South Carolina",
        "SD": "South Dakota",
        "TN": "Tennessee",
        "TX": "Texas",
        "UT": "Utah",
        "VT": "Vermont",
        "VA": "Virginia",
        "WA": "Washington",
        "WV": "West Virginia",
        "WI": "Wisconsin",
        "WY": "Wyoming",
    }
