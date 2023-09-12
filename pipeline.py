import pandas as pd
import numpy as np
import argparse

# Read the XLSX file into a DataFrame
#df = pd.read_csv("https://raw.githubusercontent.com/sriram8113/Datasets/main/ME_Database12.csv")

def extract_data(source):
    # Read the XLSX file into a DataFrame
    df = pd.read_csv(source)
    return df   

def load_data(data, target):
    data.to_csv(target, index=False)

def cleaning(data):    
    new_data = data.copy()       
    # Count the number of NaN values in each row
    nan_count = new_data.isna().sum(axis=1)
    # Drop rows with more than 8 NaN values
    new_data = new_data[nan_count <= 8]
    # Replace variations of 'Male', 'Female' with 'Male' ,'Female'(case-insensitive)    
    new_data['Gender'] = new_data['Gender'].str.replace('male', 'Male', case=False)
    new_data['Gender'] = new_data['Gender'].str.replace('female', 'Female', case=False)
    # Assuming you have a DataFrame with a column 'Name'
    # Standardize capitalization to title case    
    new_data['Name'] = new_data['Name'].str.title()    
    # cleaning branch column
    # changing all the values to same value    
    new_data['Branch'] = 'Mechanical Engineering'    
    #cleaning date of birth    
    new_data['Date of birth'] = pd.to_datetime(new_data['Date of birth'], format='%d-%m-%Y')
    # Create separate columns for year, month, and day    
    new_data['year_born'] = new_data['Date of birth'].dt.year
    new_data['month_born'] = new_data['Date of birth'].dt.month
    new_data['day_born'] = new_data['Date of birth'].dt.day 
    
    return new_data


def custom_logic(data): 
    
    new_data = data.copy()
    # Count the number of NaN values in each row
    nan_count = new_data.isna().sum(axis=1)
    # Drop rows with more than 8 NaN values
    new_data = new_data[nan_count <= 8]   
    new_data['10th percentage'] =  new_data['10th percentage'].astype(str)
    # Remove letters, percentage symbols (%), and brackets using regular expressions
    new_data['10th percentage'] = new_data['10th percentage'].str.replace(r'[a-zA-Z%()\[\]]', '', regex=True)         
    
    def conversion(value):  
        value = float(value)
        if value > 10:
            return value
        elif 0 < value <= 10:
            return value * 9.5
        elif value < 0:
            return value * 95
        else:
            return value  # Handle other cases here
    def convert(value):
        if value < 10:
            return value*10
        else :
            return value
    
    new_data['10th percentage'] = new_data['10th percentage'].apply(conversion)
    new_data['10th percentage'] = new_data['10th percentage'].apply(convert)    

    
    return new_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source", help="tource csv")
    parser.add_argument("target", help="target csv")
    args = parser.parse_args()

    print("Extracting data from {}...".format(args.source))
    df = extract_data(args.source)
    new_df = cleaning(df)
    cleaned_data = custom_logic(new_df)
    load_data(cleaned_data, args.target)
    print('complted cleaning')









