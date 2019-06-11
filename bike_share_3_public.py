# -*- coding: utf-8 -*-
"""
Created on Mon Nov 19 15:19:17 2018

@author: RMJcar
"""
from pulp import *
import pandas as pd
import timeit
#parameters
price=2
capacity=4
vot=6
starttime='2014-08-27 11:04' # in the format of 'YYYY-MM-DD HH24:MI'
endtime='2014-08-27 12:49' # in the format of 'YYYY-MM-DD HH24:MI'
stationpairdistance=1 #recommonded to be 1
extradistance=1 #recommonded to be 1
rank = 50 #recommonded to be 50
simple=True #Make False if you want all coloumns 

#identifies and creates targets for the surplus and defict bikeshare stations
start_query=timeit.default_timer()
from sqlalchemy import create_engine
passwordfile = open ("C:/Users/RMJcar/Documents/Classes/TR 6113/password.txt","r")
password=passwordfile.read()
passwordfile.close()
engine = create_engine('postgresql://......'+password+'@........')
station_query="""  drop table if exists stations;
/* query all the bike stations and calculate surplus/defict */
 create temp table stations as (
    with original as (select max(stat.available_bike_count) as max_bike_count,
	   min(stat.available_bike_count) as min_bike_count,
	   max(stat.available_dock_count) as max_dock_count,
	   min(stat.available_dock_count) as min_dock_count,
	   max(stat.available_bike_count)::float/(
	   avg(stat.available_dock_count+stat.available_bike_count)::float) as pct_bikes,
	   max(stat.available_dock_count)::float/(
	   avg(stat.available_dock_count+stat.available_bike_count)::float) as pct_docks,
	   's'||stat.station_id||'s' as station_id,
	   loc.latitude,
	   loc.longitude,
	   loc.label  
from rmj.citi_bike_stations  loc
inner join rmj.bike_station_status stat
on stat.station_id = loc.citibike_station_id
where stat.record_timestamp between to_timestamp('{}','YYYY-MM-DD HH24:MI') and 
                              to_timestamp('{}','YYYY-MM-DD HH24:MI') and
/* midtown to the battery */
loc.longitude between -74.008304 and -73.972417 and
loc.latitude between 40.702070 and 40.768077 and
position('Active' in stat.status)>0
group by  stat.station_id,
	   loc.latitude,
	   loc.longitude,
	   loc.label),
/* for each bike station, calculate how many bikes are need to recieve or give */
almost as(
select case when max_bike_count > max_dock_count then
            case when pct_bikes>.9 then round(max_bike_count*.33,0)
                 when pct_bikes>.75 then round(max_bike_count*.25,0)
			       when pct_bikes>.5  then round(max_bike_count*.1,0) end
			else 0 end as surplus,
	  case when max_dock_count > max_bike_count then
            case when pct_docks>.9 then round(max_dock_count*.33,0)
                 when pct_docks>.75 then round(max_dock_count*.25,0)
			      when pct_docks>.5  then round(max_dock_count*.1,0) end
			else 0 end as deficit,
	 original.*
from original where (pct_bikes>.5 and max_bike_count > max_dock_count) or 
                     (pct_docks>.5 and  max_dock_count > max_bike_count))
/* produced all possible combinations of deficit and surplus stations limiting
the euclidian distance to less than a mile */
select surp.station_id as surplus_station_id,
	   surp.label  as surplus_station_name,
	   def.station_id as deficit_station_id,
	   def.label  as deficit_station_name,
	   surp.latitude as surplus_latitude,
	   surp.longitude as surplus_longitude,
	   def.latitude as deficit_latitude,
	   def.longitude as deficit_longitude,
	   surp.surplus,
	   def.deficit,
	   point(surp.longitude, surp.latitude) <@> point(def.longitude, def.latitude)	as distance
from almost surp 
cross join almost def
where surp.surplus >0 and def.deficit >0 and point(surp.longitude, surp.latitude) <@> point(def.longitude, def.latitude) <= {});
select * from stations;					 
"""
station_query=station_query.format(starttime,endtime,stationpairdistance)
station_data = pd.read_sql_query(station_query, engine)

#creates the tours of bike station pairs and taxi trips
taxitrip_query=""" drop table if exists trips;
/* select all the taxi trips that both begin and end in midtown to battery park */
create temp table trips as (
select row_number() OVER () as  trip_id,
                    	     pickup_datetime,
                         dropoff_datetime,
	                      passenger_count,
                         trip_distance,
                         fare_amount,
                         pickup_longitude, 
                         pickup_latitude, 
                         dropoff_longitude, 
                         dropoff_latitude,
	                     point(pickup_longitude, pickup_latitude) <@> point(dropoff_longitude,	dropoff_latitude) as orig_distance
from rmj.taxi_trips
where pickup_datetime between to_timestamp('{}','YYYY-MM-DD HH24:MI') and 
                              to_timestamp('{}','YYYY-MM-DD HH24:MI') and
/* mid to lower manhattan */
							  dropoff_latitude between 40.702070 and 40.768077 and 
	 dropoff_longitude between -74.008304 and -73.972417 and
      pickup_latitude between 40.702070 and 40.768077 and 
	 pickup_longitude between -74.008304 and -73.972417 
);
/* create every possible combination of taxi trip and bike station pair and
also compute the first mile, last mile distance from taxi ends and bike ends */
with pairs as (
select         't'||trips.trip_id||'t' as trip_id,
		      't'||trips.trip_id||'t'||'_'||stations.surplus_station_id||'_'||stations.deficit_station_id as tour_id,
			   trips.orig_distance,
             stations.surplus_station_id,
             stations.deficit_station_id,
             trips.pickup_longitude as taxi_pickup_longitude, 
             trips.pickup_latitude as taxi_pickup_latitude, 
             trips.dropoff_longitude as taxi_dropoff_longitude, 
             trips.dropoff_latitude as taxi_dropoff_latitude,
             stations.surplus_longitude,
             stations.surplus_latitude,
             stations.deficit_longitude, 
             stations.deficit_latitude,
             trips.fare_amount,
             stations.surplus,
             stations.deficit,
			   point(trips.pickup_longitude, trips.pickup_latitude) <@> point(stations.surplus_longitude, stations.surplus_latitude) as first_mile_dist,
			   stations.distance as middle_mile_dist,
			   point(stations.deficit_longitude, stations.deficit_latitude) <@> point(trips.dropoff_longitude,	trips.dropoff_latitude) as last_mile_distance
from trips
cross join stations),
/* rank each taxi bike station pair by distance for each surplus station */
surplus as (
select pairs.*,
       first_mile_dist + middle_mile_dist + last_mile_distance as new_distance,
	   first_mile_dist + middle_mile_dist + last_mile_distance - orig_distance as additional_distance,
       row_number () over(partition by surplus_station_id order by first_mile_dist + middle_mile_dist + last_mile_distance - orig_distance) as surplus_rank
from pairs
where first_mile_dist + middle_mile_dist + last_mile_distance - orig_distance <={}),
/* rank each taxi bike station pair by distance for each deficit station station */
deficit as (
  select surplus.*,
         row_number () over(partition by deficit_station_id order by additional_distance) as deficit_rank
from surplus)
/* pull top 50 for each bike station (surplus or deficit) */
select deficit.*
from deficit
where deficit_rank<= {} or surplus_rank <={}
"""     
taxitrip_query=taxitrip_query.format(starttime,endtime,extradistance,rank,rank)
taxidata = pd.read_sql_query(taxitrip_query, engine)
stop_query=timeit.default_timer()
print('Query time: ' + str((stop_query-start_query)/60.0) + " mintutes")


#define optimization model
print('initialize')
m = pulp.LpProblem('Bike and Taxi', pulp.LpMaximize)
pairs = {}
print('create variables and objective')
for i in range(len(taxidata)):
    # create decision variables
    pairs[i]=pulp.LpVariable(taxidata.iloc[i]['tour_id'],0,1, pulp.LpBinary)
    # add to objective
m += pulp.lpSum(pairs[i]*(price*capacity -vot*taxidata.iloc[i]['additional_distance']) for i in range(len(taxidata)) )
#create constraints for taxi trip k’ can only pick up at most one set of bikes
taxitrips=taxidata['trip_id'].unique().tolist()
print('Tour only once constraint')
for i in taxitrips:
    tours=list()
    for j in range(len(pairs)):
        if str(i) in str(pairs[j]):
            tours.append(pairs[j])
    m += pulp.lpSum(tours) <= 1
# create constraints for Cannot take more than the predetermined “Excess”  number of bikes from station 
print('Excess constraint')
Excess=station_data[station_data['surplus']>0][['surplus_station_id','surplus']].copy()
Excess=Excess.drop_duplicates()
for i in range(len(Excess)):
    tours=list()
    for j in range(len(pairs)):
        if Excess.iloc[i]['surplus_station_id'] in str(pairs[j]):
            tours.append(pairs[j])
        m += capacity*pulp.lpSum(tours) <= Excess.iloc[i]['surplus']
# create constraints for Cannot put more than the predetermined “Deficit”  number of bikes from a deficit station j’
print('Deficit constraint')
Deficit=station_data[station_data['deficit']>0][['deficit_station_id','deficit']].copy()
Deficit=Deficit.drop_duplicates()
for i in range(len(Deficit)):
    tours=list()
    for j in range(len(pairs)):
        if Deficit.iloc[i]['deficit_station_id'] in str(pairs[j]):
            tours.append(pairs[j])
        m += capacity*pulp.lpSum(tours) <= Deficit.iloc[i]['deficit']
m.solve() #solve
stop_optimize=timeit.default_timer()
print('Optimize time: ' + str((stop_optimize-stop_query)/60.0) + " mintutes")

#extract variables into dictionary
varsdict = {}
for v in m.variables():
    varsdict[v.name] = v.varValue
taxidata['selected_'+str(vot)+'_'+str(price)]=taxidata.apply(lambda x:varsdict[x['tour_id']] if pd.notnull(x['tour_id']) else None, axis=1)
taxidata['Benchmark $/mile']=taxidata.apply(lambda x: x['fare_amount']/x['orig_distance'] if x['selected_'+str(vot)+'_'+str(price)]==1 else None, axis=1)
taxidata['Alternative operator $/mile']=taxidata.apply(lambda x: (x['fare_amount']+price*capacity)/(x['new_distance']) if x['selected_'+str(vot)+'_'+str(price)]==1 else None, axis=1)
taxidata['Additional Miles %']=taxidata.apply(lambda x: (x['new_distance'])/(x['orig_distance']) if x['selected_'+str(vot)+'_'+str(price)]==1 else None, axis=1)


#agg calculations
totalnumtours=len(taxidata[taxidata['selected_'+str(vot)+'_'+str(price)]==1])
totalfare=taxidata[taxidata['selected_'+str(vot)+'_'+str(price)]==1]['fare_amount'].sum()
totalorigdistance=taxidata[taxidata['selected_'+str(vot)+'_'+str(price)]==1]['orig_distance'].sum()
totalnewdistance=taxidata[taxidata['selected_'+str(vot)+'_'+str(price)]==1]['new_distance'].sum()
print('Total number of tours considered: ' + str(len(taxidata)))
print('Total number of selected tours: ' + str(totalnumtours))
print('Total number of delivered bikes: ' + str(capacity*totalnumtours))
print('Average excess distance: ' + str(taxidata[taxidata['selected_'+str(vot)+'_'+str(price)]==1]['additional_distance'].mean()))
print('Average Benchmark $/mile: ' + str(totalfare/totalorigdistance))
print('Average Alternative operator $/mile: ' + str((totalfare+price*capacity*totalnumtours)/totalnewdistance))
print('Average Additional Miles %' + str(totalnewdistance/totalorigdistance))
if simple == True:
    taxidata=taxidata[['tour_id','orig_distance','fare_amount', 'new_distance',	'selected_'+str(vot)+'_'+str(price),'Benchmark $/mile','Alternative operator $/mile','Additional Miles %']]

#Change the below to where you want the model results to be exported
taxidata.to_csv('C:/Users/RMJcar/Documents/Classes/TR-7013/Project/taxibike_out_5.csv')
station_data.to_csv('C:/Users/RMJcar/Documents/Classes/TR-7013/Project/bikestation_out_5.csv')