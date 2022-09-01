#!/usr/bin/env python3

from datetime import datetime
import gzip
import os
from sqlite3 import DataError

import pandas as pd
from unidecode import unidecode


def get_countries(
    url: str = "https://pkgstore.datahub.io/JohnSnowLabs/country-and-continent-codes-list/country-and-continent-codes-list-csv_csv/data/b7876b7f496677669644f3d1069d3121/country-and-continent-codes-list-csv_csv.csv",
) -> pd.DataFrame:
    """fetch a list of countries with their ISO codes"""
    countries = pd.read_csv(url)
    return countries


def get_owid(
    url: str = "https://covid.ourworldindata.org/data/owid-covid-data.csv",
) -> pd.DataFrame:
    """fetch COVID-19 case data from OWID"""
    owid_data = pd.read_csv(url)
    owid_data["year_mon"] = owid_data.apply(
        lambda r: "-".join(r.date.split("-")[:-1]), axis=1
    )
    return owid_data


def extract_africa_metadata(
    gisaid_metadata_filename: str, output_filename: str = "metadata.tsv"
):
    """Filters the GISAID genomic epidemiology metadata file to extract the entries from Africa"""
    if os.stat(gisaid_metadata_filename).st_mtime > os.stat(output_filename).st_mtime:
        with gzip.open(gisaid_metadata_filename, "rb") as infile:
            with open(output_filename, "w") as outfile:
                for line in infile:
                    line_str = line.decode("utf")
                    if not first_line_seen:
                        outfile.write(line_str)
                        first_line_seen = True
                    fields = line_str.split("\t")
                    total_genomes += 1
                    if fields[5] == "Africa":
                        outfile.write(line_str)


def handle_two_part_dates(date: str) -> str:
    """ turn a date in the form YYYY-MM into YYYY-MM-1 """
    parts = date.split("-")
    if len(parts) == 2:
        parts.append("1")
    date = "-".join(parts)
    return date


def is_date_valid(date: str) -> bool:
    """ checks if date is valid in YYYY-MM-DD format """
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        return False
    return True

    
def get_africa_metadata(metadata_filename: str = "metadata.tsv") -> pd.DataFrame:
    """ Extracts African metadata from GISAID metadata and does clean up"""
    africa_metadata = pd.read_csv(metadata_filename, delimiter="\t", index_col=0)
    africa_metadata.date = africa_metadata.date.apply(handle_two_part_dates)
    africa_metadata = africa_metadata[africa_metadata.date.apply(is_date_valid)]
    # only retain things with good dates
    africa_metadata = africa_metadata[
        africa_metadata.apply(lambda x: len(x["date"].split("-")) == 3, axis=1)
    ]
    # drop things without a Nextstrain clade - these are typically poor quality
    africa_metadata = africa_metadata[africa_metadata.Nextstrain_clade.notna()]

    # fix up a submitting lab names
    africa_metadata.loc[
        africa_metadata.submitting_lab
        == "KRISP, KZn Research Innovation and Sequencing Platform",
        "submitting_lab",
    ] = "KRISP, KZN Research Innovation and Sequencing Platform"
    africa_metadata.loc[
        africa_metadata.submitting_lab.isin(
            [
                "CERI, Centre for Epidemic Response and Innvoation, Stellenbosch University and KRISP, KZN Research Innovation and Sequencing Platform, UKZN.",
                "CERI, Centre for Epidemic Response and Innovation, Stellenbosch Univeristy & KRISP, KZN Research Innovation and Sequencing Platform",
                "CERI, Centre for Epidemic Response and Innovation, Stellenbosch University and CERI-KRISP, KZN Research Innovation and Sequencing Platform",
            ]
        ),
        "submitting_lab",
    ] = "CERI, Centre for Epidemic Response and Innovation"
    africa_metadata.loc[
        africa_metadata.submitting_lab.isin(
            [
                "National Health Laboratory Service/University of Cape Town (National Health Laboratory Service/University of Cape Town (NHLS/UCT))",
                "National Health Laboratory Service/University of Cape Town (NHLS/UCT)",
                "National Health Laboratory Service/UCT",
            ]
        ),
        "submitting_lab",
    ] = "NHLS/UCT"
    africa_metadata.loc[
        africa_metadata.submitting_lab.isin(
            [
                "National Health Laboratory Service (NHLS), Tygerberg",
                "Division of Medical Virology, Stellenbosch University and National Health Laboratory Service (NHLS)",
                "Division of Medical Virology, National Health Laboratory Service (NHLS), Tygerberg Hospital / Stellenbosch University",
                "Stellenbosch University and NHLS",
                "National Health Laboratory Services, Virology",
            ]
        ),
        "submitting_lab",
    ] = "Division of Medical Virology, Stellenbosch University and NHLS Tygerberg Hospital"
    africa_metadata.loc[
        africa_metadata.submitting_lab
        == "ZARV, Department Mdeical Virology, University of Pretoria",
        "submitting_lab",
    ] = "ZARV, Department Medical Virology, University of Pretoria"
    africa_metadata.loc[
        africa_metadata.submitting_lab
        == "Where sequence data have been generated and submitted to GISAID",
        "submitting_lab",
    ] = "MRC/UVRI & LSHTM Uganda Research Unit"
    africa_metadata.loc[
        africa_metadata.submitting_lab
        == "KEMRI-Wellcome Trust Research Programme,Kilifi",
        "submitting_lab",
    ] = "KEMRI-Wellcome Trust Research Programme/KEMRI-CGMR-C Kilifi"
    africa_metadata.loc[
        (africa_metadata.country == "Nigeria")
        & (
            africa_metadata.submitting_lab.str.startswith(
                "African Centre of Excellence for Genomics of Infectious Diseases"
            )
        ),
        "submitting_lab",
    ] = "ACEGID, African Centre of Excellence for Genomics of Infectious Diseases, Redeemer’s University, Ede"
    africa_metadata.loc[
        africa_metadata.submitting_lab == "Redeemer's University, ACEGID",
        "submitting_lab",
    ] = "ACEGID, African Centre of Excellence for Genomics of Infectious Diseases, Redeemer’s University, Ede"
    africa_metadata.loc[
        (africa_metadata.country == "Nigeria")
        & (africa_metadata.submitting_lab.str.startswith("National")),
        "submitting_lab",
    ] = "NCDC, National Reference Laboratory, Nigeria Centre for Disease Control, Gaduwa, Abuja, Nigeria"
    africa_metadata.loc[
        (africa_metadata.country == "Democratic Republic of the Congo")
        & (
            africa_metadata.submitting_lab
            == "Pathogen Sequencing Lab, National Institute for Biomedical Research (INRB)"
        ),
        "submitting_lab",
    ] = "INRB, Pathogen Sequencing Lab, National Institute for Biomedical Research"

    # add date year / month fields
    africa_metadata["date_yearmon"] = africa_metadata.apply(
        lambda r: "-".join(r.date.split("-")[:-1]), axis=1
    )
    africa_metadata["date_submitted_yearmon"] = africa_metadata.apply(
        lambda r: "-".join(r.date_submitted.split("-")[:-1]), axis=1
    )

    # calculate the number of days between sample collection and sample submission
    africa_metadata["days_to_submit"] = africa_metadata.apply(
        lambda r: int(
            (
                datetime.strptime(r.date_submitted, "%Y-%m-%d")
                - datetime.strptime(r.date, "%Y-%m-%d")
            ).total_seconds()
            // (3600 * 24)
        ),
        axis=1,
    )
    return africa_metadata


def get_name_to_two_letter_code(africa_metadata, countries):
    # make a dict mapping GISAID country name to ISO 2 letter code
    name_to_two_letter_code = {
        "Union of the Comoros": "KM",
        "Republic of the Congo": "CG",
        "Côte d'Ivoire": "CI",
        "Democratic Republic of the Congo": "CD",
        "Eswatini": "SZ",
        "Guinea": "GN",
    }
    for country_name in africa_metadata.country.unique():
        country_info = countries[countries.Country_Name.str.contains(country_name)]
        if len(country_info) == 1:
            name_to_two_letter_code[country_name] = country_info.iloc[
                0
            ].Two_Letter_Country_Code

    return name_to_two_letter_code

def get_nextstrain_sequence_counts(nextstrain_counts_filename, countries):
    data = pd.read_csv(nextstrain_counts_filename)
    country_name_alias = {
        "Armenia": "Armenia, Republic of",
        "Azerbaijan": "Azerbaijan, Republic of",
        "Cabo Verde": "Cape Verde, Republic of",
        "China": "China, People's Republic of",
        "Cyprus": "Cyprus, Republic of",
        "Curacao": "Curaçao",
        "Democratic Republic of the Congo": "Congo, Democratic Republic of the",
        "Republic of the Congo": "Congo, Republic of the",
        "Dominica": "Dominica, Commonwealth of",
        "Dominican Republic": "Dominican Republic",
        "Eswatini": "Swaziland, Kingdom of",
        "Georgia": "Georgia",
        "Guinea": "Guinea, Republic of",
        "India": "India, Republic of",
        "Iraq": "Iraq, Republic of",
        "Ireland": "Ireland",
        "Kazakhstan": "Kazakhstan, Republic of",
        "Kyrgyzstan": "Kyrgyz Republic",
        "Laos": "Lao People's Democratic Republic",
        "Netherlands": "Netherlands, Kingdom of the",
        "Niger": "Niger, Republic of",
        "Nigeria": "Nigeria, Federal Republic of",
        "North Macedonia": "Macedonia, The Former Yugoslav Republic of", # officially The Republic of North Macedonia
        "Russia": "Russian Federation",
        "Saudi Arabia": "Saudi Arabia, Kingdom of",
        "South Korea": "Korea, Republic of",
        "Sudan": "Sudan, Republic of",
        "Turkey": "Turkey, Republic of",
        "Union of the Comoros": "Comoros, Union of the",
        "UK": "United Kingdom of Great Britain & Northern Ireland",
        "USA": "United States of America"
    }
    continents = []
    iso_2_codes = []
    for country_name in data.country:
        continent = iso_2_code = None
        decoded_country_name = unidecode(country_name)
        if decoded_country_name in country_name_alias:
            decoded_country_name = country_name_alias[decoded_country_name]
        if (countries.Country_Name == decoded_country_name).any():
            # exact match
            country_info = countries[countries.Country_Name == decoded_country_name]
            continent = country_info.iloc[0].Continent_Name
            iso_2_code = country_info.iloc[0].Two_Letter_Country_Code
        else:
            country_info = countries[countries.Country_Name.str.contains(decoded_country_name)]
            if len(country_info) != 1:
                if decoded_country_name == 'Kosovo':
                    continent = 'Europe'
                    iso_2_code = 'XK'  # see https://en.wikipedia.org/wiki/XK_(user_assigned_code)
                elif decoded_country_name == 'Palestine':
                    continent = 'Asia'
                    iso_2_code = 'PS'  # UN Observer State of Palestine
                else:
                    raise('Unknown country:', country_name)
            else:
                continent = country_info.iloc[0].Continent_Name
                iso_2_code = country_info.iloc[0].Two_Letter_Country_Code
        continents.append(continent)
        iso_2_codes.append(iso_2_code)
    data['continent'] = continents
    data['iso_2_code'] = iso_2_codes
    return data      

gisaid_country_to_country_name = {
    'Algeria': "Algeria, People's Democratic Republic of",
    'Angola': 'Angola, Republic of',
    'Benin': 'Benin, Republic of',
    'Botswana': 'Botswana, Republic of',
    'Burkina Faso': 'Burkina Faso',
    'Burundi': 'Burundi, Republic of',
    'Cabo Verde': 'Cape Verde, Republic of',
    'Cameroon': 'Cameroon, Republic of',
    "Côte d'Ivoire": "Cote d'Ivoire, Republic of",
    'Central African Republic': 'Central African Republic',
    'Chad': 'Chad, Republic of',
    'Democratic Republic of the Congo': 'Congo, Democratic Republic of the',
    'Djibouti': 'Djibouti, Republic of',
    'Egypt': 'Egypt, Arab Republic of',
    'Equatorial Guinea': 'Equatorial Guinea, Republic of',
    'Eswatini': 'Swaziland, Kingdom of',
    'Ethiopia': 'Ethiopia, Federal Democratic Republic of',
    'Gabon': 'Gabon, Gabonese Republic',
    'Gambia': 'Gambia, Republic of the',
    'Ghana': 'Ghana, Republic of',
    'Guinea': 'Guinea, Republic of',
    'Guinea-Bissau': 'Guinea-Bissau, Republic of',
    'Kenya': 'Kenya, Republic of',
    'Lesotho': 'Lesotho, Kingdom of',
    'Liberia': 'Liberia, Republic of',
    'Libya': 'Libyan Arab Jamahiriya',
    'Madagascar': 'Madagascar, Republic of',
    'Malawi': 'Malawi, Republic of',
    'Mali': 'Mali, Republic of',
    'Mauritania': 'Mauritania, Islamic Republic of',
    'Mauritius': 'Mauritius, Republic of',
    'Morocco': 'Morocco, Kingdom of',
    'Mozambique': 'Mozambique, Republic of',
    'Namibia': 'Namibia, Republic of',
    'Niger': 'Niger, Republic of',
    'Nigeria': 'Nigeria, Federal Republic of',
    'Republic of the Congo': 'Congo, Republic of the',
    'Rwanda': 'Rwanda, Republic of',
    'Sao Tome and Principe': 'Sao Tome and Principe, Democratic Republic of',
    'Senegal': 'Senegal, Republic of',
    'Seychelles': 'Seychelles, Republic of',
    'Sierra Leone': 'Sierra Leone, Republic of',
    'Somalia': 'Somalia, Somali Republic',
    'South Africa': 'South Africa, Republic of',
    'South Sudan': 'South Sudan',
    'Sudan': 'Sudan, Republic of',
    'Tanzania': 'Tanzania, United Republic of',
    'Togo': 'Togo, Togolese Republic',
    'Tunisia': 'Tunisia, Tunisian Republic',
    'Uganda': 'Uganda, Republic of',
    'Union of the Comoros': 'Comoros, Union of the',
    'Zambia': 'Zambia, Republic of',
    'Zimbabwe': 'Zimbabwe, Republic of',    
}

def country_name_to_iso3(country_name: str, countries_df: pd.DataFrame):
    selected_countries = countries_df[countries_df.Country_Name == gisaid_country_to_country_name.get(country_name)].Three_Letter_Country_Code
    if len(selected_countries) != 1:
        raise ValueError(f"Wrong number of matches for {country_name}: {len(selected_countries)}")
    return selected_countries.iloc[0]

def get_income_groups(url: str = 'https://databankfiles.worldbank.org/data/download/site-content/CLASS.xlsx'):
    income_groups = pd.read_excel(url, usecols=['Code', 'Region', 'Income group']).set_index('Code')
    return income_groups

def insert_income_groups(df, countries_df, income_groups_df, column_name):
    df.insert(len(df.columns), 'iso3', df[column_name].apply(lambda c: country_name_to_iso3(c, countries_df)))
    df = df.join(income_groups_df, on='iso3')
    return df
