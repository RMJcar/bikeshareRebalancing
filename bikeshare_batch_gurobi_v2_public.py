# -*- coding: utf-8 -*-
"""
Created on Mon Nov 19 15:19:17 2018

@author: RMJca
"""
import itertools
from gurobipy import *
import pandas as pd
import timeit
import datetime
#parameters
price=2
capacity=4
vot=6
stationpairdistance=1 #recommonded to be 1 mile
extradistance=1 #recommonded to be 1 mile
rank = 50 #recommonded to be 50
starttimer=datetime.datetime.strptime('2014-05-01 00:01','%Y-%m-%d %H:%M') # in the format of 'YYYY-MM-DD HH24:MI'
endtimer=datetime.datetime.strptime('2014-05-01 01:00','%Y-%m-%d %H:%M') # in the format of 'YYYY-MM-DD HH24:MI'
stoptimer=datetime.datetime.strptime('2014-07-31 11:59','%Y-%m-%d %H:%M')
finaltable = pd.DataFrame(columns=['datetime', 'taxiTrips', 'surplusStations',
                                   'deficitStations','Tours','selectedTours',
                                   'deliveredBikes','avg_excessDistance',
                                   'avg_benchmark$/mile',
                                   'avg_alternativeOperator$/mile',
                                   'avg_additionalMiles %'])
adddict={'datetime':None, 'taxiTrips':None, 'surplusStations':None,
         'deficitStations':None,'Tours':None,'selectedTours':None,
         'deliveredBikes':None,'avg_excessDistance':None,
         'avg_benchmark$/mile':None,'avg_alternativeOperator$/mile':None,
         'avg_additionalMiles %':None}
indexer=1
while starttimer < stoptimer:
    starttime=starttimer.strftime('%Y-%m-%d %H:%M')
    adddict['datetime']=starttime
    endtime=endtimer.strftime('%Y-%m-%d %H:%M')
    #identifies and creates targets for the surplus and defict bikeshare stations
    start_query=timeit.default_timer()
    from sqlalchemy import create_engine
    passwordfile = # password
    password=passwordfile.read()
    passwordfile.close()
    engine = create_engine #engine 
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
    	   row_number() OVER () as station_id,
    	   loc.lat as latitude,
    	   loc.lon as longitude
    from rmj.citibike_station  loc
    inner join rmj.bike_station_status_old stat
    on stat.station_id = loc.station_id
    where stat.record_timestamp between to_timestamp('{START}','YYYY-MM-DD HH24:MI') and 
                                  to_timestamp('{END}','YYYY-MM-DD HH24:MI') and
    /* midtown to the battery */
    loc.lon between -74.008304 and -73.972417 and
    loc.lat between 40.702070 and 40.768077 and
    position('Active' in stat.status)>0
    group by  stat.station_id,
    	   loc.lat,
    	   loc.lon,
    	   loc.station_name),
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
    	   def.station_id as deficit_station_id,
    	   surp.latitude as surplus_latitude,
    	   surp.longitude as surplus_longitude,
    	   def.latitude as deficit_latitude,
    	   def.longitude as deficit_longitude,
    	   surp.surplus,
    	   def.deficit,
    	   point(surp.longitude, surp.latitude) <@> point(def.longitude, def.latitude)	as distance
    from almost surp 
    cross join almost def
    where surp.surplus >0 and def.deficit >0 and point(surp.longitude, surp.latitude) <@> point(def.longitude, def.latitude) <= {DISTANCE});
    select * from stations;					 
    """
    station_query=station_query.format(START=starttime,END=endtime,DISTANCE=stationpairdistance)
    station_data = pd.read_sql_query(station_query, engine)
    
    #creates the tours of bike station pairs and taxi trips
    taxitrip_query=""" drop table if exists trips;
    /* select all the taxi trips that both begin and end in midtown to battery park */
    create temp table trips as (
    select row_number() OVER () as  trip_id,
                        	     pickup_datetime,
                             dropoff_datetime,
                             trip_distance,
                             fare_amount,
                             pickup_longitude, 
                             pickup_latitude, 
                             dropoff_longitude, 
                             dropoff_latitude,
    	                     point(pickup_longitude, pickup_latitude) <@> point(dropoff_longitude,	dropoff_latitude) as orig_distance
    from rmj.taxi_trips
    where pickup_datetime between to_timestamp('{START}','YYYY-MM-DD HH24:MI') and 
                                  to_timestamp('{END}','YYYY-MM-DD HH24:MI') and
    /* mid to lower manhattan */
    							  dropoff_latitude between 40.702070 and 40.768077 and 
    	 dropoff_longitude between -74.008304 and -73.972417 and
          pickup_latitude between 40.702070 and 40.768077 and 
    	 pickup_longitude between -74.008304 and -73.972417
         /* and passenger_count=1 */
    );
    /* create every possible combination of taxi trip and bike station pair and
    also compute the first mile, last mile distance from taxi ends and bike ends */
    with pairs as (
    select         trips.trip_id as trip_id,
        		   trips.orig_distance,
                   trips.fare_amount,
                 stations.surplus_station_id,
                 stations.deficit_station_id,
                 -- not needed for solver, just for mapping
                 /*trips.pickup_longitude as taxi_pickup_longitude, 
                 trips.pickup_latitude as taxi_pickup_latitude, 
                 trips.dropoff_longitude as taxi_dropoff_longitude, 
                 trips.dropoff_latitude as taxi_dropoff_latitude, 
                 stations.surplus_longitude,
                 stations.surplus_latitude,
                 stations.deficit_longitude, 
                 stations.deficit_latitude, */
                 stations.surplus,
                 stations.deficit,
    			   point(trips.pickup_longitude, trips.pickup_latitude) <@> point(stations.surplus_longitude, stations.surplus_latitude) as first_mile_dist,
    			   stations.distance as middle_mile_dist,
    			   point(stations.deficit_longitude, stations.deficit_latitude) <@> point(trips.dropoff_longitude,	trips.dropoff_latitude) as last_mile_distance
    from trips
    cross join stations),
    /* rank each taxi bike station pair by distance for each surplus station */
    surplus as (
    select trip_id,
           orig_distance,
           fare_amount,
           surplus_station_id,
           deficit_station_id,
           surplus,
           deficit,
           first_mile_dist + middle_mile_dist + last_mile_distance as new_distance,
    	   first_mile_dist + middle_mile_dist + last_mile_distance - orig_distance as additional_distance,
           row_number () over(partition by surplus_station_id order by first_mile_dist + middle_mile_dist + last_mile_distance - orig_distance) as surplus_rank
    from pairs
    where first_mile_dist + middle_mile_dist + last_mile_distance - orig_distance <={DISTANCE}),
    /* rank each taxi bike station pair by distance for each deficit station station */
    deficit as (
      select surplus.*,
             row_number () over(partition by deficit_station_id order by additional_distance) as deficit_rank
    from surplus)
    /* pull top 50 for each bike station (surplus or deficit) */
    select deficit.*
    from deficit
    where deficit_rank<= {RANK} or surplus_rank <={RANK}
    """     
    taxitrip_query=taxitrip_query.format(START=starttime, END=endtime, DISTANCE=extradistance, RANK=rank, PRICE=price, CAPACITY=capacity, VOT=vot)
    taxidata = pd.read_sql_query(taxitrip_query, engine)
    if len(taxidata)>0 and len(station_data)>0:
        taxidata['tour_id']=taxidata.apply(lambda x: (int(x['trip_id']),int(x['surplus_station_id']),int(x['deficit_station_id'])), axis = 1)  
        stop_query=timeit.default_timer()
        print('Query time: ' + str((stop_query-start_query)/60.0) + " mintutes")
        #define optimization model
        print(str(len(taxidata)) +' possible tours. initialize')
        trips=taxidata[['trip_id']].copy().drop_duplicates()
        trips['tours']=trips.apply(lambda x: [], axis=1)
        trips.set_index('trip_id',inplace=True)   
        Excess=station_data[station_data['surplus']>0][['surplus_station_id','surplus']].copy().drop_duplicates()
        Excess['tours']=Excess.apply(lambda x: [], axis=1)
        Excess.set_index('surplus_station_id',inplace=True)   
        Deficit=station_data[station_data['deficit']>0][['deficit_station_id','deficit']].copy().drop_duplicates()
        Deficit['tours']=Deficit.apply(lambda x: [], axis=1)
        Deficit.set_index('deficit_station_id',inplace=True)
        for i, data in taxidata.iterrows():
            Deficit.loc[data['deficit_station_id']]['tours'].append(data['tour_id'])
            Excess.loc[data['surplus_station_id']]['tours'].append(data['tour_id'])
            trips.loc[data['trip_id']]['tours'].append(data['tour_id'])
        #Deficit['tours']=Deficit.apply(lambda x: tuple(x['tours']), axis=1)
        #Excess['tours']=Excess.apply(lambda x: tuple(x['tours']), axis=1)
        #trips['tours']=trips.apply(lambda x: tuple(x['tours']), axis=1)
        m = Model('Taxi and bikes')
        print('create variables and objective')
        x=m.addVars(taxidata['tour_id'].tolist(), name='X_', vtype=GRB.INTEGER, ub = capacity)
        y=m.addVars(taxidata['tour_id'].tolist(), name='Y_', vtype=GRB.BINARY)
        cstr1=m.addConstrs((quicksum(x[tour] for tour in Deficit.loc[station]['tours']) <= Deficit.loc[station]['deficit'] for station in tuple(Deficit.index.values.tolist())),'deficit station constraint')
        cstr2=m.addConstrs((quicksum(x[tour] for tour in Excess.loc[station]['tours']) <= Excess.loc[station]['surplus'] for station in tuple(Excess.index.values.tolist())),'surplus station constraint')
        cstr3=m.addConstrs((quicksum(y[tour] for tour in trips.loc[trip]['tours']) <= 1 for trip in tuple(trips.index.values.tolist())),'trip selection constraint')
        cstr4=m.addConstrs((x[tour] <= y[tour]*1000 for tour in tuple(taxidata['tour_id'].tolist())),'binary and integer variable constraint') 
        taxidata.set_index('tour_id',inplace=True)  
        distances=taxidata[['additional_distance']].to_dict('index').copy()
        taxidata.reset_index(inplace=True)
        obj=sum(x[tour]*price-y[tour]*vot*distances[tour]['additional_distance'] for tour in tuple(taxidata['tour_id'].tolist()))
        m.setObjective(obj,GRB.MAXIMIZE)
    
        print('solving')
        m.optimize() #solve
        stop_optimize=timeit.default_timer()
        print('Optimize time: ' + str((stop_optimize-stop_query)/60.0) + " mintutes")
        
        #extract variables into dictionary
        xdict = {}
        ydict = {}
        for tour in taxidata['tour_id'].tolist():
            xdict[tour] = x[tour].X
            ydict[tour] = y[tour].X
        taxidata['selected']=taxidata.apply(lambda x: ydict[x['tour_id']] if pd.notnull(x['tour_id']) else None, axis=1)
        taxidata['bikes']=taxidata.apply(lambda x: xdict[x['tour_id']] if pd.notnull(x['tour_id']) else None, axis=1)
        taxidata['Benchmark $/mile']=taxidata.apply(lambda x: x['fare_amount']/x['orig_distance'] if x['selected']==1 else None, axis=1)
        taxidata['Alternative operator $/mile']=taxidata.apply(lambda x: (x['fare_amount']+price*x['bikes'])/(x['new_distance']) if x['selected']==1 else None, axis=1)
        taxidata['Additional Miles %']=taxidata.apply(lambda x: (x['new_distance'])/(x['orig_distance']) if x['selected']==1 else None, axis=1)
        
        
        #agg calculations
        totalnumtours=len(taxidata[taxidata['selected']==1])
        totalbikes=taxidata[taxidata['selected']==1]['bikes'].sum()
        totalfare=taxidata[taxidata['selected']==1]['fare_amount'].sum()
        totalorigdistance=taxidata[taxidata['selected']==1]['orig_distance'].sum()
        totalnewdistance=taxidata[taxidata['selected']==1]['new_distance'].sum()
        adddict['taxiTrips']=len(trips)
        adddict['surplusStations']=len(Excess)
        adddict['deficitStations']=len(Deficit)
        adddict['Tours']=len(taxidata)
        adddict['selectedTours']=totalnumtours
        adddict['deliveredBikes']=totalbikes
        adddict['avg_excessDistance']=taxidata[taxidata['selected']==1]['additional_distance'].mean()
        adddict['avg_benchmark$/mile']=totalfare/totalorigdistance
        adddict['avg_alternativeOperator$/mile']=((totalfare+price*totalbikes)/totalnewdistance)
        adddict['avg_additionalMiles %']=totalnewdistance/totalorigdistance
        finaltable.loc[indexer]=adddict
        finaltable.to_csv('C:/Users/RMJca/Documents/Classes/TR-7013/Project/batch/'+str(indexer)+'.csv')
        indexer=indexer+1
    starttimer=starttimer+datetime.timedelta(seconds=3600)
    endtimer=endtimer+datetime.timedelta(seconds=3600)
