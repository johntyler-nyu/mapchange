#from colab notebook: https://colab.research.google.com/drive/1_dkejjFqLCBjgm5Dv9vJDq2rAnJsAt1u?usp=sharing

import pandas as pd
from google.colab import auth
auth.authenticate_user()
print('Authenticated')

#Connect to contributions table

%%bigquery --project nyu-cap-ae df
SELECT * FROM `nyu-cap-ae.junk.donors_jt`

#Connect to careers_config table

%%bigquery --project nyu-cap-ae df_careers
SELECT * FROM `nyu-cap-ae.junk.careers_config_v2`

##Create a table with 'career' and 'unique_id' (first/last/city) as two additional columns

import pandas as pd
import re

df["donor_occupation"].fillna("No Occupation Listed", inplace = True)

csv_output = pd.DataFrame() #create empty dataframe to store results

for i in range(len(df_careers)): # loop through the config, cross checking it against the data
    token = df_careers.iloc[i,0] # pull out the token individually
    occupation = df_careers.iloc[i,1] # pull out the corresponding career as well
    rows = df[df['donor_occupation'].str.contains(token)] # subsetting the data to pull only rows that hit our token
    #rows_no_token = df_test[~df_test['donor_occupation'].str.contains(token)] # subsetting the data to pull rows that do not hit this token
    rows['career'] = str(occupation) # add new column with the occupation to the subset of the data from the line above 
    csv_output = csv_output.append(rows) # append to the output dataframe created outside of the loop
    #csv_no_token = csv_no_token.append(rows_no_token) # append to the output dataframe created outside of the loop

csv_output.to_gbq(
    'elt_ohio.donors', # Temp table showing occupation category and crude unique_id
    'nyu-cap-ae',
    chunksize=None,
    if_exists='replace' #If the table already exists, the write back will fail.
)
