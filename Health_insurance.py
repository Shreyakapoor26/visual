# %%
import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# %%
#read the file
hi = pd.read_csv('HI_2021.csv')
hi.head()

# %%
# Define a function to check if a column name contains the specified string
def col_contains(col_name, string):
    return string in col_name

# Select the columns that contain either "Percent Insured!!Estimate" or "Percent Uninsured!!Estimate"
selected_cols = [col for col in hi.columns if col_contains(col, "Percent Insured!!Estimate") or col_contains(col, "Percent Uninsured!!Estimate")]
selected_cols

# %%
# Add the "Label (Grouping)" column to the list of selected columns
selected_cols.append("Label (Grouping)")

# Create a new DataFrame containing only the selected columns
insured_unisured_df = hi[selected_cols]
insured_unisured_df.head()

# %%
# make the last column first
cols = insured_unisured_df.columns.tolist()
cols = cols[-1:] + cols[:-1]
insured_unisured_df = insured_unisured_df[cols]
insured_unisured_df.head(2)

# %%
transposed_df = insured_unisured_df.transpose()
transposed_df.head()

# %%
# Rename the first row to be the column names
new_header = transposed_df.iloc[0] #grab the first row for the header
transposed_df = transposed_df[1:] #take the data less the header row
transposed_df.columns = new_header #set the header row as the df header
transposed_df.head()

# %%
# make index the first column
transposed_df.reset_index(inplace=True)

# %%
# rename Label (Grouping)
transposed_df.rename(columns={'index': 'State'}, inplace=True)
transposed_df['Insurance Type'] = transposed_df['State']
transposed_df['State'] = transposed_df['State'].str.split('!!').str[0]
transposed_df['Insurance Type'] = transposed_df['Insurance Type'].str.split('!!').str[1]
transposed_df.head()

# %%
# make Insurance Type second column it is last right now
cols = transposed_df.columns.tolist()
cols = cols[:1] + cols[-1:] + cols[1:-1]
transposed_df = transposed_df[cols]
transposed_df.head()

# %%
# find all columns
transposed_df.columns

# strip any white space in the column names
transposed_df.columns = transposed_df.columns.str.strip()
transposed_df.columns

# %%
# delete multiple columns Civilian noninstitutionalized population and AGE
cleaned_df = transposed_df.drop(columns=['AGE', 'Civilian noninstitutionalized population'])
cleaned_df.head(2)

# %%
# download this to csv
cleaned_df.to_csv('cleaned_df.csv', index=False)

# %%
# melt the dataframe
age_df = pd.melt(cleaned_df, id_vars=['State', 'Insurance Type'], value_vars=[
                                                               'Under 19 years', 
                                                               '19 to 64 years',
                                                              '65 years and older' ], var_name='AGE', value_name='age_percent')


age_df.head(2)

# %%
sex_df = pd.melt(cleaned_df, id_vars=['State', 'Insurance Type'], value_vars=['Male',
                                                            'Female'], var_name='SEX', value_name='sex_percent')

sex_df.head(2)

# %%
race_df = pd.melt(cleaned_df, id_vars=['State', 'Insurance Type'], value_vars=['White alone',
       'Black or African American alone',
       'American Indian and Alaska Native alone', 'Asian alone',
       'Native Hawaiian and Other Pacific Islander alone',
       'Some other race alone', 'Two or more races',
       'Hispanic or Latino (of any race)',
       'White alone, not Hispanic or Latino'], var_name='RACE', value_name='race_percent')

race_df.head(2)


# %%
citizenship_df = pd.melt(cleaned_df, id_vars=['State', 'Insurance Type'], value_vars=['Native born', 'Foreign born',
       'Naturalized', 'Not a citizen'], var_name='NATIVITY AND U.S. CITIZENSHIP STATUS', value_name='citizenship_percent')

citizenship_df.head(2)


# %%
education_df = pd.melt(cleaned_df, id_vars=['State', 'Insurance Type'], value_vars=["Less than high school graduate", 
                                                                                    'High school graduate (includes equivalency)',
                                                                                    "Some college or associate's degree", 
                                                                                    "Bachelor's degree or higher"], var_name='EDUCATIONAL ATTAINMENT', value_name='education_percent')

education_df.head(2)

# %%
# merge all the dataframes
merged_e_c_df = pd.merge(education_df, citizenship_df, on=['State', 'Insurance Type'])
merged_s_a_df = pd.merge(sex_df, age_df, on=['State', 'Insurance Type'])
merged_all_df = pd.merge(merged_s_a_df, citizenship_df, on=['State', 'Insurance Type'])
merged_all_df.head(2)

# %%
#rename State to state
merged_all_df.rename(columns={'State': 'state'}, inplace=True)

# %%
#plot usa map with altair 
import altair as alt
from vega_datasets import data

state_map = data.us_10m.url
state_map

# merge id with state name
state_id = data.population_engineers_hurricanes()[['state', 'id']]
merged_with_id = merged_all_df.merge(state_id, on='state')
merged_with_id.head(2)

# %%
# Split the data into two dataframes, one for insured and one for uninsured
insured_df = merged_with_id[merged_with_id['Insurance Type'] == 'Percent Insured']
uninsured_df = merged_with_id[merged_with_id['Insurance Type'] == 'Percent Uninsured']

# %%
# download insured_df
insured_df.to_csv('insured_df.csv', index=False)

# %%
insured_df.shape

# %%
# take percent sign out of the percent columns sex_percent, age_percent, citizenship_percent
insured_df = insured_df.replace('%','', regex=True)
insured_df.rename(columns={'NATIVITY AND U.S. CITIZENSHIP STATUS': 'CITIZENSHIP'}, inplace=True)
insured_df.head(20)

# %%
# Define the selection for state field
select_state = alt.selection_single(fields=['state'])

# Define the layer chart with cases
map_layer = alt.Chart(alt.topo_feature(state_map, 'states')).mark_geoshape().project('albersUsa').transform_lookup(
    lookup='id',
    from_=alt.LookupData(data=insured_df, key='id', fields=['age_percent', 'state']),).encode(
    alt.Color('age_percent:Q', scale=alt.Scale(scheme='yellowgreenblue')),
    tooltip=['state:N', 'age_percent:Q'],
    fillOpacity=alt.condition(select_state, alt.value(1), alt.value(0.3))).add_selection(
    select_state
    ).properties(
    title= "Percent of Insured population",
    width=1400,
    height=600,
    )

# Define the barplot chart to show race percent by state
age_barplot = alt.Chart(insured_df).mark_bar().encode(
    y=alt.Y('state:N', sort=alt.EncodingSortField('age_percent', order='descending')),
    x='age_percent:Q',
    color='AGE:N',
    opacity=alt.condition(select_state, alt.value(1), alt.value(0.3)),
    tooltip=['state:N', 'AGE:N', 'age_percent:Q']
).properties(title= "AGE").add_selection(select_state).transform_filter(
    select_state
)

# Define the barplot chart to show race percent by state
citizenship_barplot = alt.Chart(insured_df).mark_bar().encode(
    y=alt.Y('state:N', sort=alt.EncodingSortField('citizenship_percent', order='descending')),
    x='citizenship_percent:Q',
    color='CITIZENSHIP:N',
    opacity=alt.condition(select_state, alt.value(1), alt.value(0.3)),
    tooltip=['state:N', 'CITIZENSHIP:N', 'citizenship_percent:Q']
).properties(title= "NATIVITY AND U.S. CITIZENSHIP STATUS").add_selection(select_state).transform_filter(
    select_state
)

# Define the barplot chart to show sex percent by state
sex_barplot = alt.Chart(insured_df).mark_bar().encode(
    y=alt.Y('state:N', sort=alt.EncodingSortField('sex_percent', order='descending')),
    x='sex_percent:Q',
    color='SEX:N',
    opacity=alt.condition(select_state, alt.value(1), alt.value(0.3)),
    tooltip=['state:N', 'SEX:N', 'sex_percent:Q']
).properties(title= "Sex percentage").add_selection(select_state).transform_filter(
    select_state
)

# Combine the charts and configure the view
dashboard = alt.vconcat(map_layer, alt.hconcat(age_barplot, citizenship_barplot, sex_barplot)).configure_view(stroke='transparent').configure_title(fontSize=30)
dashboard



