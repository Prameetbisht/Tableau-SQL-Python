import mysql.connector
import pandas as pd
import numpy as np
from tqdm import tqdm

import mysql.connector

cnx = mysql.connector.connect(user='root', password='Perry@123',
                              host='127.0.0.1',
                              database='fifa')
cursor = cnx.cursor()

df = pd.read_csv('fifa18_clean.csv', usecols=np.arange(0, 27))
df.head()

df.dropna(inplace=True, axis=0)
df.isna().sum()

# cursor.execute('CREATE TABLE player (Wage float,Value float,Name varchar(50), Age float,Nationality varchar(50), '
#                'Overall float, Potential float, Club varchar(50), Special float,Acceleration float,Aggression float,'
#                'Agility float, Balance float,Ball_control float,Composure float, Crossing float, Curve float,'
#                'Dribbling float,Finishing float,Free_kick_accuracy float, GK_diving float,GK_handling float,'
#                'GK_kicking float, GK_positioning float, GK_reflexes float,Heading_accuracy float,Interceptions '
#                'float)')

add_query = ("INSERT INTO fifa.player"
             "(Wage ,Value,Name,Age,Nationality,Overall,Potential,Club,Special,Acceleration,Aggression,Agility,Balance,\
              Ball_control,Composure,Crossing,Curve,Dribbling,Finishing,Free_kick_accuracy,GK_diving,GK_handling,\
              GK_kicking,GK_positioning,GK_reflexes,Heading_accuracy,Interceptions) "
             "VALUES (%(Wage)s, %(Value)s,%(Name)s,%(Age)s,%(Nationality)s,%(Overall)s,%(Potential)s,%(Club)s,"
             "%(Special)s, %(Acceleration)s,%(Aggression)s,%(Agility)s,%(Balance)s,%(Ball_control)s,%(Composure)s,"
             "%(Crossing)s,%(Curve)s,%(Dribbling)s,%(Finishing)s,%(Free_kick_accuracy)s,%(GK_diving)s,"
             "%(GK_handling)s,%(GK_kicking)s,%(GK_positioning)s,%(GK_reflexes)s, %(Heading_accuracy)s,"
             "%(Interceptions)s)")
for row in tqdm(df.itertuples()):
    data_fifa = {
        'Wage': row.Wage,
        'Value': row.Value,
        'Name': row.Name,
        'Age': row.Age,
        'Nationality': row.Nationality,
        'Overall': row.Overall,
        'Potential': row.Potential,
        'Club': row.Club,
        'Special': row.Special,
        'Acceleration': row.Acceleration,
        'Aggression': row.Aggression,
        'Agility': row.Agility,
        'Balance': row.Balance,
        'Ball_control': row.Ball_control,
        'Composure': row.Composure,
        'Crossing': row.Crossing,
        'Curve': row.Curve,
        'Dribbling': row.Dribbling,
        'Finishing': row.Finishing,
        'Free_kick_accuracy': row.Free_kick_accuracy,
        'GK_diving': row.GK_diving,
        'GK_handling': row.GK_handling,
        'GK_kicking': row.GK_kicking,
        'GK_positioning': row.GK_positioning,
        'GK_reflexes': row.GK_reflexes,
        'Heading_accuracy': row.Heading_accuracy,
        'Interceptions': row.Interceptions,
    }
    cursor.execute(add_query, data_fifa)
cnx.commit()
