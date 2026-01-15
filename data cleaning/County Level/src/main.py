import pandas as pd
from src.utils import *
from src.CONSTANTS import get_file_path

bounding_box = pd.read_csv(get_file_path("bounding_boxes"), dtype={"FIPS State": str, "FIPS County": str})

def load_data(
    race_type = 'DEC', election_type = 'Democrat', education_type = '18-24', solar_type = 'all', electric_customer_class= 'both', electric_dataset='NREL'
):
    # Normalized data
    wind = get_wind(get_file_path('Wind'), bounding_box) 
    gdp = get_GDP(get_file_path('GDP'), bounding_box, get_file_path('population_data'))

    if solar_type == 'all':
        solar_all= get_solar(get_file_path('Solar'), bounding_box, size='all')
        solar_small = get_solar(get_file_path('Solar'), bounding_box, size='small')
        solar_medium = get_solar(get_file_path('Solar'), bounding_box, size='medium')
        solar_large = get_solar(get_file_path('Solar'), bounding_box, size='large')
        solar = {
            'all': solar_all,
            'small': solar_small,
            'medium': solar_medium,
            'large': solar_large
        }
    elif solar_type == 'small_only':
        solar = get_solar(get_file_path('Solar'), bounding_box, size='small')
    elif solar_type == 'medium_only':
        solar = get_solar(get_file_path('Solar'), bounding_box, size='medium')
    elif solar_type == 'large_only':
        solar = get_solar(get_file_path('Solar'), bounding_box, size='large')
    elif solar_type == 'all_only':
        solar = get_solar(get_file_path('Solar'), bounding_box, size='all')
    else:
        raise ValueError(f"Invalid solar type: {solar_type}")
    
    # Non-normalized data
    private_schools = get_no_priv_schools(get_file_path('private_schools'))
    income = get_income(get_file_path('income'))
    unemployment = get_unemployment(get_file_path('unemployment'))
    if electric_dataset == 'NREL':
        electric = NREL_Electric(get_file_path('NREL_Electric'))
    elif electric_dataset == 'EIA':
        electric = get_electric(get_file_path('electric'), electric_customer_class)
        
    # Solar Roof
    solar_roof = get_solar_roof_data(get_file_path('solar_roof'), bounding_box)

    
    # Race
    if race_type == 'DEC':
        race = get_race_dec(get_file_path("DEC_race"))
    elif race_type == 'ACS':
        race = get_race_acs(get_file_path("ACS_race"))
    else:
        raise ValueError(f"Invalid data type: {race_type}")
        
    # Election
    if election_type == 'Democrat':
        election = get_election(get_file_path("election"), party='Democrat')
    elif election_type == 'Republican':
        election = get_election(get_file_path("election"), party='Republican')
    elif election_type == 'Other':
        election = get_election(get_file_path("election"), party='Other')
    elif election_type == 'Green':
        election = get_election(get_file_path("election"), party='Green')
    elif election_type == 'Libertarian':
        election = get_election(get_file_path("election"), party='Libertarian')
    elif election_type == 'all':
        # Get all election data returned as a dictionary
        election = get_election(get_file_path("election"), party='all')
    else:
        raise ValueError(f"Invalid party type: {election_type}")
        
    # Education
    if education_type == "18-24":
        education = get_education_18_24(get_file_path("education"))
    elif education_type == "25+":
        education = get_education_25_over(get_file_path("education"))
    elif education_type == "all":
        education_18_24 = get_education_18_24(get_file_path("education"))
        education_25_over = get_education_25_over(get_file_path("education"))
    else:
        raise ValueError(f"Invalid education type: {education_type}")
    
    # Merge all normalized data
    merged = merged_normalized_data(wind, gdp, solar, bounding_box)
    
    # Merge Private Schools
    merged = merged.merge(private_schools, on=["State", "County Name"], how='outer')
    
    # Merge Income
    merged = merged.merge(income, on=["State", "County Name"], how='outer')
    
    # Merged Unemployment
    merged = merged.merge(unemployment, on=["State", "County Name"], how='outer')
    
    # Merge Race
    merged = merged.merge(race, on=["State", "County Name"], how='outer')
    
    # Merge Solar Roof
    merged = merged.merge(solar_roof, on=["State", "County Name"], how='outer')
    
    # Merge Election
    if type(election) == dict:
        for key in election.keys():
            merged = merged.merge(election[key], on=["State", "County Name"], how='outer')
    else:
        merged = merged.merge(election, on=["State", "County Name"], how='outer')
        
    # Merge Education
    if education_type == "all":
        merged = merged.merge(education_18_24, on=["State", "County Name"], how='outer')
        merged = merged.merge(education_25_over, on=["State", "County Name"], how='outer')
    else:
        merged = merged.merge(education, on=["State", "County Name"], how='outer')
        
    # Merge Electric
    if electric_dataset == 'NREL':
        merged = merged.merge(electric, on=["State", "County Name"], how='outer')
    elif electric_dataset == 'EIA':
        if electric_customer_class == 'both':
            for k in electric.keys():
                merged = merged.merge(electric[k], on=["State", "County Name"], how='outer')
        else:
            merged = merged.merge(electric, on=["State", "County Name"], how='outer')
    
    return merged
    
    
    
    
