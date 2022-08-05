
from pydoc import classname
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as pexpr
import zipfile

### Config reader 

# Scenario keywords
def get_data_inbetween_date(dataframe, from_date, to_date):
    return None

###################

#### Connect to database
with zipfile.ZipFile("Monthly_count.zip","r") as zip_ref:
    zip_ref.extractall("csv/")

with zipfile.ZipFile("Sensor_location.zip","r") as zip_ref:
    zip_ref.extractall("csv/")

## Get dataframe associate of each scenario
location_info = pd.read_csv('csv/Pedestrian_Counting_System_-_Monthly__counts_per_hour_.csv')
location_sensor_info = pd.read_csv('csv/Pedestrian_Counting_System_-_Sensor_Locations.csv')

## Preprocess datetime
location_info['Date_Time'] = pd.to_datetime(location_info['Date_Time'])


merge_pandas = location_info.merge(location_sensor_info, how='inner', left_on='Sensor_ID', right_on='sensor_id')
# Filter to get Active Sensor
active_sensor_df = merge_pandas[merge_pandas['status']=='A'].reset_index(drop=True)

# Question 1
# Get the daily pedestrian counts since of all time that is available
days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
daily_count = active_sensor_df.groupby(['Day','Sensor_ID','Sensor_Name'],as_index=False).agg({'Hourly_Counts': 'sum'}).sort_values(by='Day',ascending=False)


# Question 2
# Get the monthly pedestrian counts since of all time that is available
months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
monthly_count= active_sensor_df.groupby(['Month','Sensor_ID','Sensor_Name'],as_index=False).agg({'Hourly_Counts': 'sum'}).sort_values(by='Month',ascending=False)##Scenario 3

# Question 3
# Retrive dataframe in period before lockdown
lockdown_df = active_sensor_df[active_sensor_df.Date_Time > 'January 1, 2020'].reset_index(drop=True)
lockdown_df_sum_per_month = lockdown_df.groupby(['Sensor_ID','Sensor_Name','Year','Month'],as_index=False).agg({'Hourly_Counts': 'sum'})
lockdown_df_sum_per_month['month_year'] = pd.to_datetime(lockdown_df_sum_per_month.Year.astype(str) + '/' + lockdown_df_sum_per_month.Month.astype(str) + '/01')

# Question 4
past_year_df = active_sensor_df[active_sensor_df.Date_Time >= pd.to_datetime('2021-06-01')].reset_index(drop=True)
past_year_df_sum_per_month = past_year_df.groupby(['Sensor_ID','Sensor_Name','Year','Month'],as_index=False).agg({'Hourly_Counts': 'sum'})
past_year_df_sum_per_month['month_year'] = pd.to_datetime(past_year_df_sum_per_month.Year.astype(str) + '/' + past_year_df_sum_per_month.Month.astype(str) + '/01')




external_stylesheets = ['assets/s1.css']
app=dash.Dash(__name__,external_stylesheets=external_stylesheets)
app.title='Pedestrian Count Analysis'




# Process all keys value in dropdown_mapping before run to reduce time loading
fig_sc1={}
for i in days:
    df_day = daily_count[daily_count.Day==i].sort_values(by='Hourly_Counts',ascending=False).head(10)
    fig_sc1[i]= pexpr.line(df_day, x='Sensor_Name', y= "Hourly_Counts", title="Daily Pedestrian Top 10 location count")


fig_sc2 = {}
for i in months:
    df_month = monthly_count[monthly_count.Month==i].sort_values(by='Hourly_Counts',ascending=False).head(10)
    fig_sc2[i]= pexpr.line(df_month, x='Sensor_Name', y= "Hourly_Counts", title="Monthly Pedestrian Top 10 location count")

most_change = -1
sensor_list = list(lockdown_df_sum_per_month.Sensor_ID.unique())
rate_of_change_sensors = []
for i in sorted(sensor_list):
    data_sensor = {}
    temp_df = lockdown_df_sum_per_month[lockdown_df_sum_per_month.Sensor_ID == i]
    latest_data = temp_df[temp_df.month_year == temp_df.month_year.max()]
    oldest_data = temp_df[temp_df.month_year == temp_df.month_year.min()]
    if (latest_data.month_year.iloc[0] <= pd.to_datetime('2022-05-01')):
        continue
    if (oldest_data['month_year'].iloc[0] >=  pd.to_datetime('2021-01-01')):
        print(f'Data in sensor {i} is within the the lockdown period')
        continue
    change = (oldest_data['Hourly_Counts'].iloc[0]-latest_data['Hourly_Counts'].iloc[0])/oldest_data['Hourly_Counts'].iloc[0] * 100
    data_sensor = {'sensor_id': i, 'Sensor_Name': latest_data['Sensor_Name'].iloc[0], 'oldest_date': oldest_data['month_year'].iloc[0], 'latest_date': latest_data['month_year'].iloc[0], 'Percentage_changes': change}
    rate_of_change_sensors.append(data_sensor)

change_pandemic_df = pd.DataFrame.from_records(rate_of_change_sensors)
top_20 = change_pandemic_df.head(20)
fig_sc3 = pexpr.bar(top_20.sort_values('Percentage_changes', ascending=False), x='Sensor_Name', y= "Percentage_changes", title="Rate of change in Pedestrian count")


most_change = -1
sensor_list = list(past_year_df_sum_per_month.Sensor_ID.unique())
rate_of_change_sensors= []
for i in sorted(sensor_list):
    data_sensor = {}
    temp_df = past_year_df_sum_per_month[past_year_df_sum_per_month.Sensor_ID == i]
    latest_data = temp_df[temp_df.month_year == temp_df.month_year.max()]
    oldest_data = temp_df[temp_df.month_year == temp_df.month_year.min()]
    if (latest_data.month_year.iloc[0] <= pd.to_datetime('2022-05-01')):
        continue
    change = (latest_data['Hourly_Counts'].iloc[0]-oldest_data['Hourly_Counts'].iloc[0])/latest_data['Hourly_Counts'].iloc[0] * 100
    data_sensor = {'sensor_id': i, 'Sensor_Name': latest_data['Sensor_Name'].iloc[0], 'oldest_date': oldest_data['month_year'].iloc[0], 'latest_date': latest_data['month_year'].iloc[0], 'Percentage_changes': change}
    rate_of_change_sensors.append(data_sensor)

change_past_year_df = pd.DataFrame.from_records(rate_of_change_sensors)
top_20_past_year = change_past_year_df.head(20)
fig_sc4 = pexpr.bar(top_20_past_year.sort_values('Percentage_changes', ascending=False), x='Sensor_Name', y= "Percentage_changes", title="Rate of change in Pedestrian count")



app.layout=html.Div(
                children=[
                        html.H1('Pedestrian Count analysis by Dash',style={ 'textAlign': 'center'},className='title'),
                            dcc.Tab(label='Scenario 1',children=[
                                html.Div(children=[
                                    html.Div(children=[(
                                        html.Div([ html.H4('Select the day to see'),
                                                dcc.Dropdown(id = 'day',
                                                options=days,value=days[0])
                                                ],className='pretty_container twelve columns'
                                                )),
                                        ]
                                    ,className='row container-display'),
                                    
                                html.Div([
                                    dcc.Graph(id='fig_daily')
                                ],className='pretty_container twelve columns')]),
                                html.Div(children=[
                                    html.Div(children=[(
                                        html.Div([ html.H4('Select the month to see'),
                                                dcc.Dropdown(id = 'month',
                                                options=months,value=months[0])
                                                ],className='pretty_container twelve columns'
                                                )),
                                        ]
                                    ,className='row container-display'),
                                    
                                html.Div([
                                    dcc.Graph(id='fig_monthly',style={'autosize':True})
                                ],className='pretty_container twelve columns')]),
                                html.Div([
                                    dcc.Graph(figure=fig_sc3,style={'autosize':True})
                                ],className='pretty_container twelve columns'),
                                html.Div([
                                    dcc.Graph(figure=fig_sc4,style={'autosize':True})
                                ],className='pretty_container twelve columns')
                        ],className='twelve columns'),                   
                       
                    ], className = 'twelve columns')

@app.callback(
    Output('fig_daily', 'figure'),
    [Input('day', 'value')])
def update_daily_plot(day):
    return fig_sc1[day]

@app.callback(
    Output('fig_monthly', 'figure'),
    [Input('month', 'value')])
def update_monthly_plot(month):
    return fig_sc2[month]

if __name__=='__main__':
    app.run_server(debug=False,threaded=True)


  