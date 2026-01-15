import geopandas as gpd
import pandas as pd
from src.helpers import *

state_abbreviations = get_state_abbr()
FIPS = FIPS_getter()


#### WIND CLEANING #####
def get_wind(datapath, fixed_BB):
    """
    Function to get the wind data

    datapath: path to the wind original wind data (i.e the shapefile)
    fixed_BB: the fixed bounding box dataframe
    """
    data = gpd.read_file(datapath)
    wind_sum_mw = (
        data[["county", "statename", "wind_mw"]]
        .groupby(["statename", "county"])
        .sum()
        .reset_index()
        .rename(
            columns={
                "statename": "State",
                "county": "County Name",
                "wind_mw": "total_wind_mw",
            }
        )
    )
    wind_avg_mw = (
        data[["county", "statename", "wind_mw"]]
        .groupby(["statename", "county"])
        .mean()
        .reset_index()
        .rename(
            columns={
                "statename": "State",
                "county": "County Name",
                "wind_mw": "avg_wind_mw",
            }
        )
    )
    wind_project_count = (
        data.groupby(["statename", "county"])
        .count()
        .reset_index()[["statename", "county", "plant_code"]]
        .rename(
            columns={
                "statename": "State",
                "county": "County Name",
                "plant_code": "wind_count",
            }
        )
    )

    wind_df = (
        wind_sum_mw.merge(fixed_BB, on=["State", "County Name"], how="left")
        .merge(wind_avg_mw, on=["State", "County Name"], how="left")
        .merge(wind_project_count, on=["State", "County Name"], how="left")
        .fillna(0)
    )

    wind_df["Wind Capacity Intensity (MW / 1000 sq mile)"] = (
        wind_df["total_wind_mw"] / wind_df["area mi2"] * 1000
    )
    wind_df["Wind Project Intensity (Projects / 1000 sq mile)"] = (
        wind_df["wind_count"] / wind_df["area mi2"] * 1000
    )

    wind_df["Wind Avg Capacity Intensity (MW / 1000 sq mile)"] = (
        wind_df["avg_wind_mw"] / wind_df["area mi2"] * 1000
    )

    wind_df = wind_df.drop(
        columns=["GEOID", "total_wind_mw", "wind_count", "avg_wind_mw"]
    )

    return wind_df

#### Solar Roof Data #####
def get_solar_roof_data(datapath, fixed_BB):
    data = pd.read_csv(datapath)
    solar_roof = data.groupby(["region_name", "state_name"]).sum().reset_index()[['region_name', 'state_name', 'existing_installs_count', 'kw_total', 'kw_median']]

    solar_roof = solar_roof.rename(columns = {'region_name': 'County Name', 'state_name': 'State', 'existing_installs_count': 'Number of Existing Installs', 'kw_total': 'Total Installed Capacity (kW)', 'kw_median': 'Median Installed Capacity (kW)'})

    solar_roof['County Name'] = solar_roof['County Name'].str.replace(' County', '').str.replace(' Parish', '').str.replace(".", "")
    
    # Merge wth the fixed bounding box to get the area
    solar_roof = solar_roof.merge(fixed_BB, on=['State', 'County Name'], how='inner')
    
    # Normalize by area
    solar_roof['Total Installed Capacity (kW/ 1000 sq mile)'] = solar_roof['Total Installed Capacity (kW)'] / solar_roof['area mi2'] * 1000
    
    solar_roof['Median Installed Capacity (kW / sq mile)'] = solar_roof['Median Installed Capacity (kW)'] / solar_roof['area mi2']
    
    solar_roof['Number of Existing Installs / sq mile'] = solar_roof['Number of Existing Installs'] / solar_roof['area mi2']
    
    # Round to 2 decimal places
    solar_roof['Total Installed Capacity (kW/ 1000 sq mile)'] = solar_roof['Total Installed Capacity (kW/ 1000 sq mile)'].round(2)
    solar_roof['Median Installed Capacity (kW / sq mile)'] = solar_roof['Median Installed Capacity (kW / sq mile)'].round(2)
    solar_roof['Number of Existing Installs / sq mile'] = solar_roof['Number of Existing Installs / sq mile'].round(2)
    
    solar_roof = solar_roof.drop(columns=['GEOID', 'area mi2', 'area km2', 'FIPS State', 'FIPS County'])
    
    return solar_roof


#### GDP CLEANING #####


def get_GDP(datapath, fixed_BB, pop_data):
    """
    Function to get the GDP data normalized by area

    datapath: path to the GDP data (cleaned csv)
    fixed_BB: the fixed bounding box dataframe
    """
    gdp_data = pd.read_csv(datapath, dtype={'GeoFIPS': str})
    pop_data = pd.read_csv(pop_data, dtype={"STATE": str, "COUNTY": str})
    gdp_data['Description'] = gdp_data['Description'].str.strip()
    gdp_data = gdp_data[gdp_data["Description"] == "Real GDP (thousands of chained 2017 dollars)"]
    gdp_data['GeoFIPS'] = gdp_data['GeoFIPS'].str.strip().str.replace('"', '')
    gdp_data['County FIPS'] = gdp_data['GeoFIPS'].str[2:]
    gdp_data['State FIPS'] = gdp_data['GeoFIPS'].str[:2]
    gdp_data = gdp_data.merge(fixed_BB, left_on=['State FIPS', 'County FIPS'], right_on=['FIPS State', 'FIPS County'], how='inner')
    gdp_data = gdp_data.merge(pop_data, left_on=['State FIPS', 'County FIPS'], right_on=['STATE', 'COUNTY'], how='inner')
    gdp_data_important = gdp_data[["State", "County Name", "2017", '2018', '2019', '2020', '2021', '2022', 'POPESTIMATE2022']]
    rename_dict = {
        '2017': 'GDP_2017',
        '2018': 'GDP_2018',
        '2019': 'GDP_2019',
        '2020': 'GDP_2020',
        '2021': 'GDP_2021',
        '2022': 'GDP_2022',
    }
    gdp_data_important = gdp_data_important.rename(columns=rename_dict)
    # divide all numerical columns by county area
    for col in rename_dict.values():
        gdp_data_important[col] = gdp_data_important[col].astype("float32")
        gdp_data_important[col] = gdp_data_important[col] / (gdp_data_important["POPESTIMATE2022"])
        # round to 2 decimal places
        gdp_data_important[col] = gdp_data_important[col].round(2)
        
    gdp_data_important = gdp_data_important.rename(columns={'POPESTIMATE2022': 'Population Estimate'})
        
    return gdp_data_important


#### Solar Cleaning #####


def get_solar(datapath, fixed_BB, size="all"):
    """
    Function to get the solar data normalized by area

    datapath: path to the solar data
    fixed_BB: the fixed bounding box dataframe
    """
    solar = pd.read_csv(datapath)
    solar = solar[["statename", "county", "solar_mw"]]

    if size == "all":
        solar_grouped = solar.groupby(["statename", "county"])
    elif size == "small":
        solar_grouped = solar[solar["solar_mw"] < 5].groupby(["statename", "county"])
    elif size == "medium":
        solar_grouped = solar[
            (solar["solar_mw"] >= 5) & (solar["solar_mw"] < 25)
        ].groupby(["statename", "county"])
    elif size == "large":
        solar_grouped = solar[solar["solar_mw"] >= 25].groupby(["statename", "county"])
    else:
        raise ValueError(f"Invalid size type: {size}")

    solar_sum_all = solar_grouped.sum().reset_index()
    solar_avg_all = solar_grouped.mean().reset_index()
    solar_count_all = solar_grouped.count().reset_index()

    solar_merged = solar_sum_all.merge(
        solar_avg_all, on=["statename", "county"], suffixes=("_sum", "_avg")
    ).merge(solar_count_all, on=["statename", "county"])

    solar_merged.columns = [
        "state",
        "county",
        "solar_mw_sum_" + size,
        "solar_mw_avg_" + size,
        "solar_mw_count_" + size,
    ]

    solar_with_area = (
        solar_merged.rename(columns={"state": "State", "county": "County Name"})
        .merge(fixed_BB, on=["State", "County Name"])
        .drop(columns=["GEOID"])
    )
    solar_with_area["Solar MW 1000 sq mile " + size] = (
        solar_with_area["solar_mw_sum_" + size] / solar_with_area["area mi2"] * 1000
    )
    solar_with_area["Solar Projects 1000 sq mile " + size] = (
        solar_with_area["solar_mw_count_" + size] / solar_with_area["area mi2"] * 1000
    )

    solar_with_area["Solar MW Avg 1000 sq mile " + size] = (
        solar_with_area["solar_mw_avg_" + size] / solar_with_area["area mi2"] * 1000
    )

    return solar_with_area[
        [
            "State",
            "County Name",
            "Solar MW 1000 sq mile " + size,
            "Solar Projects 1000 sq mile " + size,
            "Solar MW Avg 1000 sq mile " + size,
        ]
    ]


#### ELECTRIC CLEANING ####
def get_electric(datapath, customer_class):
    data = pd.read_csv(datapath, dtype={"utility_id_eia": str})  # read data
    # Get the important columns
    data = data[
        ["utility_id_eia", "customer_class", "customers", "sales_mwh", "sales_revenue"]
    ]

    # As the county FIPS in the pudl data is the state and county number merge create that column
    FIPS_temp = FIPS.copy()
    FIPS_temp["FIPS Full"] = (
        FIPS_temp["FIPS State"].astype(int).astype(str) + FIPS_temp["FIPS County"]
    )
    FIPS_temp = FIPS_temp.drop(columns=["FIPS State", "FIPS County"])

    # Merge to get the county names
    data_with_county = data.merge(
        EIA_FIPS_getter(), on="utility_id_eia", how="left"
    ).drop(columns=["utility_id_eia"])

    # Get the commercial data only and the residential data only
    data_com = data_with_county[data_with_county["customer_class"] == "commercial"]
    data_res = data_with_county[data_with_county["customer_class"] == "residential"]

    # Grouby the county_fips_id and sum to get the total electric price consumptions
    data_com_summed = (
        data_com.groupby("county_id_fips")
        .sum()
        .reset_index()
        .drop(columns=["customer_class"])
    )
    data_res_summed = (
        data_res.groupby("county_id_fips")
        .sum()
        .reset_index()
        .drop(columns=["customer_class"])
    )

    # Merge with FIPS to get the county and state name for the respective county fips id
    data_com_summed = data_com_summed.merge(
        FIPS_temp, left_on="county_id_fips", right_on="FIPS Full", how="left"
    )
    data_res_summed = data_res_summed.merge(
        FIPS_temp, left_on="county_id_fips", right_on="FIPS Full", how="left"
    )
    
    # Rename for Commercial
    data_com_summed = data_com_summed.rename(columns={'sales_revenue': 'Commercial Sales Revenue', 'sales_mwh': 'Commercial Sales MWH', 'customers': 'No. Commercial Customers'}).drop(columns=['FIPS Full', 'county_id_fips'])
    # Rename for Residential
    data_res_summed = data_res_summed.rename(columns={'sales_revenue': 'Residential Sales Revenue', 'sales_mwh': 'Residential Sales MWH', 'customers': 'No. Residential Customers'}).drop(columns=['FIPS Full', 'county_id_fips'])
    
    if customer_class == 'commercial':
        return data_com_summed
    elif customer_class == 'residential':
        return data_res_summed
    elif customer_class == 'both':
        return {'commercial': data_com_summed, 'residential': data_res_summed}
    else:
        raise ValueError(f"Invalid customer class {customer_class}")
    

def NREL_Electric(datapath):
    data = pd.read_csv(datapath)
    NREL_AVG = data[['State', 'County Name', 'comm_rate', 'ind_rate', 'res_rate']].groupby(['State', 'County Name']).mean().reset_index()
    
    rename_dict = {
        "comm_rate": "Electric Commercial Rate",
        "ind_rate": "Electric Industrial Rate",
        "res_rate": "Electric Residential Rate"
    }
    NREL_AVG = NREL_AVG.rename(columns=rename_dict)
    
    return NREL_AVG


#### Education Level Cleaning #####
def get_education_18_24(datapath):
    data = pd.read_csv(datapath)

    # make the first row the header as first row contains more meaningful column names
    data.columns = data.iloc[0]
    data = data[1:]
    # drop the last column due to random nan value
    data = data.iloc[:, :-1]

    important_columns_18_24 = (
        data.columns.str.contains("Estimate")
        & data.columns.str.contains("18 to 24 years")
        & data.columns.str.contains("Percent")
        & ~data.columns.str.contains("Male")
        & ~data.columns.str.contains("Female")
    )

    rename_dict = {
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 18 to 24 years!!Less than high school graduate": "18-24 Less than high school graduate",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 18 to 24 years!!High school graduate (includes equivalency)": "18-24 High school graduate",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 18 to 24 years!!Some college or associate's degree": "18-24 Some college or associate's degree",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 18 to 24 years!!Bachelor's degree or higher": "18-24 Bachelor's degree or higher",
    }

    # get all columns that include the word 'estimate' and '18 to 24 years'
    data_18_24_estimates = data.loc[:, important_columns_18_24]
    state_counties = data.loc[:, ["Geographic Area Name", "Geography"]]
    state_counties["FIPS State"] = state_counties["Geography"].apply(
        lambda x: x.split("US")[1][:2]
    )
    state_counties["FIPS County"] = state_counties["Geography"].apply(
        lambda x: x.split("US")[1][2:]
    )

    data_18_24_estimates = (
        pd.concat([state_counties, data_18_24_estimates], axis=1)
        .drop(
            columns=[
                "Geographic Area Name",
                "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 18 to 24 years",
            ],
            axis=1,
        )
        .rename(columns=rename_dict)
    )
    data_18_24_estimates = data_18_24_estimates.merge(
        FIPS,
        left_on=["FIPS State", "FIPS County"],
        right_on=["FIPS State", "FIPS County"],
        how="inner",
    )
    data_18_24_estimates = data_18_24_estimates.drop(
        columns=["FIPS State", "FIPS County", "Geography"]
    )

    return data_18_24_estimates


def get_education_25_over(datapath):
    data = pd.read_csv(datapath)

    # make the first row the header as first row contains more meaningful column names
    data.columns = data.iloc[0]
    data = data[1:]
    # drop the last column due to random nan value
    data = data.iloc[:, :-1]

    important_columns_25_over = (
        data.columns.str.contains("Estimate")
        & data.columns.str.contains("25 years")
        & data.columns.str.contains("Percent")
        & ~data.columns.str.contains("Male")
        & ~data.columns.str.contains("Female")
        & ~data.columns.str.contains("MEDIAN")
    )

    rename_dict_25_over = {
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!Less than 9th grade": "25+ Less than 9th grade",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!9th to 12th grade, no diploma": "25+ 9th to 12th grade, no diploma",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!High school graduate (includes equivalency)": "25+ High school graduate",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!Some college, no degree": "25+ Some college, no degree",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!Associate's degree": "25+ Associate's degree",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!Bachelor's degree": "25+ Bachelor's degree",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!Graduate or professional degree": "25+ Graduate or professional degree",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!High school graduate or higher": "25+ High school graduate or higher",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!Bachelor's degree or higher": "25+ Bachelor's degree or higher",
    }

    # get all columns that include the word 'estimate' and '18 to 24 years'
    data_25_over_estimates = data.loc[:, important_columns_25_over]
    state_counties = data.loc[:, ["Geographic Area Name", "Geography"]]
    state_counties["FIPS State"] = state_counties["Geography"].apply(
        lambda x: x.split("US")[1][:2]
    )
    state_counties["FIPS County"] = state_counties["Geography"].apply(
        lambda x: x.split("US")[1][2:]
    )

    data_25_over_estimates = (
        pd.concat([state_counties, data_25_over_estimates], axis=1)
        .drop(
            columns=[
                "Geographic Area Name",
                "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over",
            ],
            axis=1,
        )
        .rename(columns=rename_dict_25_over)
    )

    data_25_over_estimates = data_25_over_estimates.merge(
        FIPS,
        left_on=["FIPS State", "FIPS County"],
        right_on=["FIPS State", "FIPS County"],
        how="inner",
    )
    data_25_over_estimates = data_25_over_estimates.drop(
        columns=["FIPS State", "FIPS County", "Geography"]
    )

    return data_25_over_estimates


#### Private Schools #####
def get_no_priv_schools(datapath):
    data = pd.read_csv(datapath, dtype={"CNTY": str, "STFIP": str})
    data_clean = data[["NAME", "STFIP", "CNTY"]].copy()

    # Get the County FIPS which is the last 3 digits of the STFIP
    data_clean["FIPS County"] = data_clean["CNTY"].apply(lambda x: str(x)[-3:])
    data_clean = data_clean.rename(columns={"STFIP": "FIPS State"})

    no_priv_sch = (
        data_clean.groupby(["FIPS State", "FIPS County"])
        .count()
        .reset_index()[["FIPS State", "FIPS County", "CNTY"]]
        .rename(columns={"CNTY": "No. of Private Schools"})
    )
    no_priv_sch["FIPS State"] = no_priv_sch["FIPS State"]
    no_priv_sch = no_priv_sch.merge(
        FIPS,
        left_on=["FIPS State", "FIPS County"],
        right_on=["FIPS State", "FIPS County"],
        how="inner",
    )
    no_priv_sch = no_priv_sch.drop(columns=["FIPS State", "FIPS County"])

    return no_priv_sch


#### Race Distribution #####
def get_race_dec(datapath):
    df = pd.read_csv(datapath)  # read the data
    df.columns = df.iloc[0]
    df = df[1:]
    col_of_interest = (
        list(df.columns[1:4])
        + [col for col in df.columns[:-1] if "Population of one race:!" in col]
        + [
            " !!Total:!!Not Hispanic or Latino:!!Population of two or more races:!!Population of two races:"
        ]
    )
    df_totals = df[col_of_interest + ["Geography"]].drop(
        columns=["Geographic Area Name"]
    )
    df_totals.columns = (
        df_totals.columns.str.replace("!!Total:", "Total")
        .str.replace("!!Not Hispanic or Latino", "")
        .str.strip()
    )
    df_totals = df_totals.set_index("Geography")
    df_totals = df_totals.apply(to_int)
    df_totals = (
        df_totals.div(df_totals["Total"], axis=0).drop(columns=["Total"]).reset_index()
    )
    df_totals["FIPS State"] = df_totals["Geography"].apply(
        lambda x: x.split("US")[1][:2]
    )

    df_totals["FIPS County"] = df_totals["Geography"].apply(
        lambda x: x.split("US")[1][2:]
    )

    mapper = {
        "Total!!Hispanic or Latino": "Hispanic/Latino",
        "Total:!!Population of one race:!!White alone": "White",
        "Total:!!Population of one race:!!Black or African American alone": "Black/African American",
        "Total:!!Population of one race:!!American Indian and Alaska Native alone": "American Indian/Alaska Native",
        "Total:!!Population of one race:!!Asian alone": "Asian",
        "Total:!!Population of one race:!!Native Hawaiian and Other Pacific Islander alone": "Native Hawaiian/Other Pacific Islander",
        "Total:!!Population of one race:!!Some Other Race alone": "Others",
    }
    df_cleaned = df_totals.rename(columns=mapper).drop(columns=["Geography"])
    df_cleaned["Others"] = (
        df_cleaned["Others"]
        + df_cleaned[
            "Total:!!Population of two or more races:!!Population of two races:"
        ]
    )
    df_cleaned = df_cleaned.drop(
        columns=["Total:!!Population of two or more races:!!Population of two races:"]
    )

    df_cleaned = df_cleaned.merge(
        FIPS,
        left_on=["FIPS State", "FIPS County"],
        right_on=["FIPS State", "FIPS County"],
        how="inner",
    )
    df_cleaned = df_cleaned.drop(columns=["FIPS State", "FIPS County"])
    return df_cleaned


def get_race_acs(datapath):
    df = pd.read_csv(datapath)  # read the data
    # maje first row the header
    df.columns = df.iloc[0]
    df = df[1:].drop(columns=[df.columns[-1]])
    estimate_cols = [col for col in df.columns if "Estimate" in col]
    df_estimates = df[["Geography"] + estimate_cols[: len(estimate_cols) - 2]]
    df_estimates.columns = df_estimates.columns.str.replace(
        "Estimate!!", ""
    ).str.replace("Total:!!", "")
    # divide all columns by the total population
    df_estimates = df_estimates.set_index("Geography")
    df_estimates = df_estimates.apply(to_int)
    df_estimates = (
        df_estimates.div(df_estimates["Total:"], axis=0)
        .drop(columns=["Total:"])
        .reset_index()
    )
    df_estimates["FIPS State"] = df_estimates["Geography"].apply(
        lambda x: x.split("US")[1][:2]
    )
    df_estimates["FIPS County"] = df_estimates["Geography"].apply(
        lambda x: x.split("US")[1][2:]
    )

    mapper = {
        "Total:": "Total",
        "White alone": "White",
        "Black or African American alone": "Black/African American",
        "American Indian and Alaska Native alone": "American Indian/Alaska Native",
        "Asian alone": "Asian",
        "Native Hawaiian and Other Pacific Islander alone": "Native Hawaiian/Other Pacific Islander",
    }
    df_estimates["Other"] = (
        df_estimates["Some other race alone"] + df_estimates["Two or more races:"]
    )
    df_cleaned = df_estimates.drop(
        columns=["Geography", "Some other race alone", "Two or more races:"]
    ).rename(columns=mapper)

    df_cleaned = df_cleaned.merge(
        FIPS,
        left_on=["FIPS State", "FIPS County"],
        right_on=["FIPS State", "FIPS County"],
        how="inner",
    )
    df_cleaned = df_cleaned.drop(columns=["FIPS State", "FIPS County"])

    return df_cleaned


#### Election Distribution #####


def get_election(datapath, party="all"):
    data = pd.read_csv(datapath, dtype={"county_fips": str})
    data["county_fips"] = data["county_fips"].str[:-2]
    data["FIPS County"] = data["county_fips"].apply(lambda x: str(x)[-3:].strip())

    # state is not including the last three digits of the fips code
    data["FIPS State"] = data["county_fips"].apply(lambda x: str(x)[:-3].strip())
    county_vote = data[
        [
            "FIPS County",
            "FIPS State",
            "candidate",
            "mode",
            "party",
            "candidatevotes",
            "totalvotes",
        ]
    ]
    FIPS_int = FIPS.copy()
    FIPS_int["FIPS State"] = FIPS_int["FIPS State"].astype(int).astype(str)

    county_vote = (
        county_vote.groupby(["FIPS County", "FIPS State", "party"]).sum().reset_index()
    )

    county_vote = county_vote.merge(
        FIPS_int,
        left_on=["FIPS State", "FIPS County"],
        right_on=["FIPS State", "FIPS County"],
        how="inner",
    )

    # Check of FIPS 1 and 001 is in the df
    county_vote = county_vote.drop(columns=["FIPS State", "FIPS County"])

    county_vote["percentage_vote"] = (
        county_vote["candidatevotes"] / county_vote["totalvotes"]
    )
    county_vote = county_vote.drop(columns=["candidatevotes", "totalvotes"])

    county_vote_democrat = county_vote[county_vote["party"] == "DEMOCRAT"][
        ["State", "County Name", "percentage_vote"]
    ].rename({"percentage_vote": "democrat_percentage_vote"}, axis=1)
    county_vote_republican = county_vote[county_vote["party"] == "REPUBLICAN"][
        ["State", "County Name", "percentage_vote"]
    ].rename({"percentage_vote": "republican_percentage_vote"}, axis=1)
    county_vote_green = county_vote[county_vote["party"] == "GREEN"][
        ["State", "County Name", "percentage_vote"]
    ].rename({"percentage_vote": "green_percentage_vote"}, axis=1)
    county_vote_libertarian = county_vote[county_vote["party"] == "LIBERTARIAN"][
        ["State", "County Name", "percentage_vote"]
    ].rename({"percentage_vote": "libertarian_percentage_vote"}, axis=1)
    county_vote_other = county_vote[
        (county_vote["party"] != "DEMOCRAT")
        & (county_vote["party"] != "REPUBLICAN")
        & (county_vote["party"] != "GREEN")
        & (county_vote["party"] != "LIBERTARIAN")
    ][["State", "County Name", "percentage_vote"]].rename(
        {"percentage_vote": "other_percentage_vote"}, axis=1
    )

    if party == "all":
        data_dict = {
            "democrat": county_vote_democrat,
            "republican": county_vote_republican,
            "green": county_vote_green,
            "libertarian": county_vote_libertarian,
            "other": county_vote_other,
        }
        return data_dict

    if party == "Democrat":
        return county_vote_democrat
    elif party == "Republican":
        return county_vote_republican
    elif party == "Green":
        return county_vote_green
    elif party == "Libertarian":
        return county_vote_libertarian
    elif party == "Other":
        return county_vote_other
    else:
        return "Invalid party. Please choose from 'Democrat', 'Republican', 'Green', 'Libertarian', 'Other'"


#### Income Distribution #####
def get_income(datapath):
    data = pd.read_csv(datapath)
    # make first row the header
    data.columns = data.iloc[0]
    data = data[1:].drop(columns=[data.columns[-1]])
    estimate_cols = [
        col
        for col in data.columns
        if "Estimate" in col and "Households" in col and "Median" in col
    ]
    df_estimates = data[["Geography"] + estimate_cols]
    df_income_clean = df_estimates.rename(
        columns={"Estimate!!Households!!Median income (dollars)": "Median Income"}
    )
    df_income_clean["FIPS State"] = df_income_clean["Geography"].apply(
        lambda x: x.split("US")[1][:2]
    )
    df_income_clean["FIPS County"] = df_income_clean["Geography"].apply(
        lambda x: x.split("US")[1][2:]
    )

    df_income_clean = df_income_clean.merge(
        FIPS,
        left_on=["FIPS State", "FIPS County"],
        right_on=["FIPS State", "FIPS County"],
        how="inner",
    )
    df_income_clean = df_income_clean.drop(columns=["FIPS State", "FIPS County"])

    df_income_clean = df_income_clean.drop(columns=["Geography"]).drop_duplicates()
    return df_income_clean


#### Unemployment data #####
def get_unemployment(datapath):
    data = pd.read_csv(datapath)
    # make the first row the header
    data.columns = data.iloc[0]
    data = data[1:]
    # remove the nan which is the last column
    data = data.iloc[:, :-1]

    # Get the columns we want "unemployment rate"
    cols = [
        x
        for x in data.columns
        if "Population 16 years and over" in x
        and "AGE" not in x
        and "RACE" not in x
        and "Labor" not in x
    ]
    data = data[["Geography"] + cols]
    data_important = data[
        ["Geography"]
        + [
            "Estimate!!Total!!Population 16 years and over",
            "Estimate!!Unemployment rate!!Population 16 years and over",
        ]
    ]

    data_important = data_important.rename(
        columns={
            "Estimate!!Total!!Population 16 years and over": "Total Unemployment",
            "Estimate!!Unemployment rate!!Population 16 years and over": "Unemployment Rate",
        }
    )
    # split the geography column into state and county
    data_important["FIPS State"] = data_important["Geography"].apply(
        lambda x: x.split("US")[1][:2]
    )
    data_important["FIPS County"] = data_important["Geography"].apply(
        lambda x: x.split("US")[1][2:]
    )
    data_important = data_important.drop(columns=["Geography"])

    data_important = data_important.merge(
        FIPS,
        left_on=["FIPS State", "FIPS County"],
        right_on=["FIPS State", "FIPS County"],
        how="inner",
    )
    data_important = data_important.drop(columns=["FIPS State", "FIPS County"])
    # reorder columns
    data_important = data_important[
        ["State", "County Name", "Total Unemployment", "Unemployment Rate"]
    ]

    return data_important


#### Rural Urban Coverage #####

def get_rural_urban_coverage(datapath):
    data = pd.read_csv(datapath, dtype={"STATE": str, "COUNTY": str})
    
    data_important = data[['STATE', 'COUNTY', "ALAND_PCT_RUR", "ALAND_PCT_URB"]]
    
    data_merged = pd.merge(FIPS, data_important, left_on=['FIPS State', 'FIPS County'], right_on=['STATE', 'COUNTY'], how='inner')
    data_merged = data_merged.drop(columns=['STATE', 'COUNTY', 'FIPS State', 'FIPS County'])
    
    data_merged['ALAND_PCT_RUR'] = data_merged['ALAND_PCT_RUR'].str.rstrip('%').astype('float') / 100.0
    data_merged['ALAND_PCT_URB'] = data_merged['ALAND_PCT_URB'].str.rstrip('%').astype('float') / 100.0
    
    rename_dict = {
        'ALAND_PCT_RUR': 'Rural Area Percentage',
        'ALAND_PCT_URB': 'Urban Area Percentage'
    }
    
    data_merged = data_merged.rename(columns=rename_dict)
    
    return data_merged
    
    
    

    