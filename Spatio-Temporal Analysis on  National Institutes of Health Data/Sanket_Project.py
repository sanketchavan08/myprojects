




import pandas as pd
import numpy as np
import math



from datetime import datetime
start = datetime.now()

###############################
###CSV read
#################################

df = pd.read_csv('tripends_GPS.csv')


###########################################
##Data cleaning, filering and preprocessing
##########################################

df = df.iloc[:,[0,1,2,5,6,7]]
df = df[(df['Weeks'].isin( [32]))]  
df['Unnamed: 0'] = df['Unnamed: 0'].astype(str)       
df['RecordTime'] = pd.to_datetime(df['RecordTime'])            ###Date time conversion
df = df.reset_index(drop=True)


#####################################
####Data frame for final outcome
################################
pre_final = pd.DataFrame(df.loc[0,:])              ### take first row of main dataframe
pre_final = pre_final.T                            #### transponse
pre_final.insert(0,'name',1)                       ### to incorcoporate logic for merging

pre_final_1 = pd.DataFrame(df.loc[[0],:])   
pre_final_1.insert(0,'name',1)

final_dataframe = pre_final.merge(pre_final_1,left_on ='name', right_on = 'name',how = 'left')

l = list(df['Unnamed: 0'])

for i in range(0,df.shape[0]):
    
    lat_user1 = df.loc[i,['lat_rad']]    ##user1 latitude
    lon_user1 = df.loc[i,['lon_rad']]    ###user1 longitude
    
    zm = pd.DataFrame(df.loc[i,:])
    zm = zm.T
    zm.insert(0,'name',1)        
    #################
    ##time and distance check and filtering
    #################    
    #df2 = df[df['RecordTime']== df.loc[i,'RecordTime']]
    z = pd.DataFrame(df[ (abs(( (df['RecordTime'] - (df.loc[i,['RecordTime']]).values) /np.timedelta64(1,'h'))*60)) < 40])
    z['time_diff'] = (abs(( (z['RecordTime'] - (df.loc[i,['RecordTime']]).values) /np.timedelta64(1,'h'))*60))
    
    z = z[((np.arccos(np.sin(z['lat_rad']) * math.sin(lat_user1)    +
         np.cos(z['lat_rad'])*math.cos(lat_user1)* (z['lon_rad']     -
        (df.loc[i,['lon_rad']].values)).apply(lambda x: math.cos(x)))*6371)*1000) < 250]
    z = z[z['Email'].values != df.loc[i,['Email']].values]
    
    z.reset_index(drop=True)
    z.insert(0,'name',1)
    if (z.shape[1] == 1):
        z = z.T

    if (z.shape[0]>0):
        l[i] = pd.DataFrame(zm.merge(z,left_on ='name', right_on = 'name',how = 'left'))
        final_dataframe = l[i].append(final_dataframe,ignore_index=True)
    


final_dataframe = final_dataframe.sort_values(by=['time_diff','Weeks_x','RecordTime_x'])
final_dataframe = final_dataframe.reset_index(drop=True)

final_dataframe = final_dataframe.iloc[::2]

final_dataframe = final_dataframe.iloc[:,[0,1,2,3,6,7,8,9,10,13]]
final_dataframe = final_dataframe.dropna(axis=0,how='any')
final_dataframe = final_dataframe.reset_index(drop=True)

final_dataframe.to_csv('final_output.csv')


group_by = final_dataframe.groupby(['Email_x','Email_y']).size()
group_by.to_csv('groupby.csv')

final_dataframe.columns
lat_lon_users = final_dataframe.iloc[:,[0,1,4,6,8]]

lat_lon_users.to_csv('lat_lon_users.csv')

end = datetime.now()
print ('Total_Duration: {}'.format(end - start))
