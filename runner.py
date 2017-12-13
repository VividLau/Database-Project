import os
import json
import psycopg2 as psql
import pymongo
import pandas as pd
import gmplot
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


# function for connecting postgresql and mongodb.
# cursor and and a mongodb collection are returned.
def connect(userid, password):

    conn = psql.connect(dbname='postgres', user=userid, host='localhost', password=password)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    client = pymongo.MongoClient("localhost", 27017)
    db = client.complaint
    collection = db.coordinate

    return cur, collection


# first query function.
def summary(cursor):
    print('Summary the public safety by borough')
    while True:
        year = str(input("Please enter the year (2012-2016) e.g. '2015'; 'quit' to return to menu: "))
        if year.upper() == 'QUIT':
            break
        elif year.isdigit() is False or (int(year) > 2016) or (int(year) < 2012):
            print('Invalid input, please try again')
            print()
            continue

        sql = """select *
from (select Cast(count(law) as int) as FELONY, boro from complaint
where law ='FELONY' and extract(year from fr_dt)=%s
group by boro)aa natural join (select Cast(count(law) as int) as VIOLATION, boro from complaint
							where law ='VIOLATION' and extract(year from fr_dt)=%s
							group by boro)bb natural join (select Cast(count(law) as int) as MISDEMEANOR, boro from complaint
															where law ='MISDEMEANOR' and extract(year from fr_dt)=%s
                                                           group by boro)cc natural join ( select Cast(count(*) as int) as car_accident,boro , cast((sum(person_killed)+sum(ped_killed)+sum(motor_killed)) as int) as deadman
                                                                                           from motor natural join region
                                                                                           where extract(year from _date)=%s
                                                                           group by boro)dd
"""
        arg = year
        
        # use execute(query, argument) to avoid injection.
        cursor.execute(sql,(arg,arg,arg,arg))
        list1 = cursor.fetchall()
        col_names = []
        for name in cursor.description:
            col_names.append(name[0])
        df = pd.DataFrame(list1, columns=col_names)
        df.index += 1
        print(df)
        print()


# function for second query
def mostvtype(cursor):
    print("This function can show you how many motor collisions happened and how many people got hurt caused by a certain reason")
    input("Press 'Enter' to continue")
    print()
    print('Waiting for plotting table...')
    print()
    sql = """ select  distinct(contributingv1) as contributes from motor where contributingv1 !='NULL' """
    cursor.execute(sql)
    list2 = cursor.fetchall()
    col_names = []
    for name in cursor.description:
        col_names.append(name[0])
    
    # pandas package is applied for pretty print of table.
    df = pd.DataFrame(list2, columns=col_names)
    df.index += 1
    print(df)
    while True:
        words = str(input("Please choose a collision contributor e.g. '1' for Unsafe Speed; "
                          "'quit' to return to menu: "))
        if words.upper() == 'QUIT':
            break
        elif words.isdigit() is False or (int(words)>len(list2)) or (int(words)<0):
            print('Invalid input! Please try again.')
            print()
            continue

        sql=""" select Cast(extract(year from _date) as int) as year,count(*) as accidents, sum(person_injured+ped_injured+cyc_injured+motor_injured) as injured, sum(person_killed+ped_killed+cyc_killed+motor_killed) as fatalities
from motor natural join region
where UPPER(contributingv1) like UPPER(%s) or  UPPER(contributingv2) like UPPER(%s) or  UPPER(contributingv3) like UPPER(%s) or  UPPER(contributingv4) like UPPER(%s) or  UPPER(contributingv5) like UPPER(%s)
group by extract(year from _date) """
        arg= list2[int(words)-1]
        cursor.execute(sql,(arg,arg,arg,arg,arg))
        list1 = cursor.fetchall()
        print('number of people got killed because of ',end=' ')
        print(arg)
        col_names = []
        for name in cursor.description:
            col_names.append(name[0])
        df = pd.DataFrame(list1, columns=col_names)
        df.index += 1
        print(df)
        print()


# function for the third query
def law(cursor):
    print('This function can show you how long will it take for a person to report to the police after the crime happened')
    print()
    while True:
        sql = """ select distinct(law) from complaint """
        cursor.execute(sql)
        list2 = cursor.fetchall()
        col_names = []
        for name in cursor.description:
            col_names.append(name[0])
        df = pd.DataFrame(list2, columns=col_names)
        df.index += 1
        print(df)
        print()
        words = str(input("Which one are you interesting with e.g. '1' for felony; 'quit' to return to menu: "))
        print()
        if words.upper() == 'QUIT':
            break
        elif words.isdigit() is False or (int(words)>len(list2)) or (int(words)<0):
            continue

        sql = """
        select ofns_desc as crimes, Cast(avg(rpt_dt-fr_dt) as int ) as days
        from complaint natural join offense_cd
        where  law like UPPER(%s)
        group by ofns_desc 
        """
        arg=[list2[int(words)-1]]
        cursor.execute(sql,arg)
        list2 = cursor.fetchall()
        col_names = []
        for name in cursor.description:
            col_names.append(name[0])
        df = pd.DataFrame(list2, columns=col_names)
        df.index += 1
        print(df)
        print()


# function for the fourth query
def crimedifferenttime(cursor):
    print('You can enter a time zone, and see the top 5 most frequently happened crimes in this time period.')
    print()
    while True:
        tim1 = str(input("Please enter a beginning time(0-24) e.g. '2' for 2 a.m; 'quit' to return to menu: "))
        if tim1.upper() == 'QUIT':
            break
        elif tim1.isdigit() is False or (int(tim1)>24) or (int(tim1)<0):
            print('Invalid input! Please try again.')
            print()
            continue

        tim2 = str(input("Please enter a ending time(0-24): "))

        if tim2.isdigit() is False or (int(tim2)>24) or (int(tim2)<0):
            print('Invalid input! Please try again.')
            print()
            continue
        elif int(tim1) > int(tim2):
            tmp =tim2
            tim2 =tim1
            tim1 = tmp
        time1 = tim1+":00"
        time2 = tim2+":00"
        sql="""select ofns_desc as crimes, Cast(count(ofns_desc) as int) from complaint natural join offense_cd
            where fr_tm > %s and fr_tm < %s
            group by ofns_desc
            order by count(ofns_desc) desc
            limit 5"""
        cursor.execute(sql,(time1,time2))
        list1 = cursor.fetchall()
        col_names = []
        for name in cursor.description:
            col_names.append(name[0])
        df = pd.DataFrame(list1, columns=col_names)
        df.index += 1
        print(df)
        print()


# function to load data into postgresql and mongodb.
# the whole process include loading and normalizing.
# the whole process will take about 6 mins.
def load_data(cursor, client):

    print('Loading...')
    print()
    cursor.execute("""
    DROP TABLE IF EXISTS complaint;
    DROP TABLE IF EXISTS motor;
    CREATE TABLE complaint
    (
        cmplnt_num INT PRIMARY KEY,
        fr_dt CHAR(15),
        fr_tm TIME,
        to_dt CHAR(15),
        to_tm TIME,
        rpt_dt CHAR(15),
        ky_cd INT,
        ofns_desc VARCHAR(80),
        pd_cd INT,
        pd_desc VARCHAR(80),
        cptd CHAR(9),
        law CHAR(12),
        juris VARCHAR(50),
        boro CHAR(13),
        addr INT,
        occ CHAR(12),
        prem_typ VARCHAR(50),
        park_nm VARCHAR(80),
        hadevelop VARCHAR(80),
        x_coor INT,
        y_coor INT,
        lat DECIMAL(12,8),
        long DECIMAL(12,8),
        lat_long VARCHAR(80)
    );

    CREATE TABLE motor
    (
        _date CHAR(15),
        _time TIME,
        boro CHAR(13),
        zip CHAR(5),
        lat DECIMAL(12,8),
        long DECIMAL(12,8),
        location VARCHAR(30),
        on_street VARCHAR(60),
        cross_street VARCHAR(60),
        off_street VARCHAR(60),
        person_injured INT,
        person_killed INT,
        ped_injured INT,
        ped_killed INT,
        cyc_injured INT,
        cyc_killed INT,
        motor_injured INT,
        motor_killed INT,
        contributingV1 VARCHAR(80),
        contributingV2 VARCHAR(80),
        contributingV3 VARCHAR(80),
        contributingV4 VARCHAR(80),
        contributingV5 VARCHAR(80),
        uni_key INT PRIMARY KEY,
        vtype1 VARCHAR(60),
        vtype2 VARCHAR(60),
        vtype3 VARCHAR(60),
        vtype4 VARCHAR(60),
        vtype5 VARCHAR(60)
    );
    """
                   )
                   
    # read data in-memory and use copy_expert() function.
    # user postgres have no permission to open the file in many directory
    # by using normal "COPY" sql command
    with open("NYPD_Complaint_Data_Historic.csv", 'r') as f1:
        cursor.copy_expert("COPY complaint FROM STDIN WITH CSV HEADER DELIMITER AS ','", f1)

    with open("NYPD_Motor_Vehicle_Collisions.csv", 'r') as f2:
        cursor.copy_expert("COPY motor FROM STDIN WITH CSV HEADER DELIMITER AS ','", f2)

    # normalize the ralation.
    cursor.execute("""
    ALTER TABLE complaint ALTER COLUMN fr_dt TYPE DATE using to_date(fr_dt, 'MM/DD/YYYY');
    ALTER TABLE complaint ALTER COLUMN to_dt TYPE DATE using to_date(to_dt, 'MM/DD/YYYY');
    ALTER TABLE complaint ALTER COLUMN rpt_dt TYPE DATE using to_date(rpt_dt, 'MM/DD/YYYY');
    ALTER TABLE motor ALTER COLUMN _date TYPE DATE using to_date(_date, 'MM/DD/YYYY');

    DELETE FROM complaint WHERE fr_dt < '2012-01-01';
    ALTER TABLE complaint DROP x_coor, DROP y_coor, DROP hadevelop, DROP juris, DROP lat_long,
    DROP addr, DROP pd_cd, DROP pd_desc;
    ALTER TABLE motor DROP location;

    DROP TABLE IF EXISTS offense_cd;
    DROP TABLE IF EXISTS region;
    CREATE TABLE offense_cd ( ky_cd INT, ofns_desc VARCHAR(80) );
    INSERT INTO offense_cd select distinct ky_cd, ofns_desc from complaint where ofns_desc is not NULL;
    CREATE TABLE region ( lat float, long float, zip char(5), boro char(13) );
    INSERT INTO region select distinct lat, long, zip, boro from motor where lat > 0 and zip is not NULL;
    """
                   )

    # select some attribute from sql relation for mongoDB.
    # write in-memory and export as .csv file.
    sql = "select lat, long, cmplnt_num, ofns_desc, fr_dt from complaint"
    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(sql)
    with open('coordinate.csv', 'w') as f:
        cursor.copy_expert(outputquery, f)

    # import .csv file into mongoDB
    cdir = os.path.dirname(__file__)
    file_res = os.path.join(cdir, 'coordinate.csv')

    data = pd.read_csv(file_res)
    data_json = json.loads(data.to_json(orient='records'))
    client.remove()
    client.insert(data_json)

    cursor.execute("""
    ALTER TABLE complaint DROP ofns_desc, DROP lat, DROP long;
    ALTER TABLE motor DROP zip, DROP boro;
    """
                   )

    print('Loading complete!')
    print()


# The last query function, combine SQL and NOsql database.
# Query the coordinate information in MongoDB according to the query outcome from PostgresDB.
# Plot the coordinate into Google map and form a heat map.
def heatmap(cursor, mongo):
    
    print('This function will show a table of top 15 most frequently happened crimes,')
    print('and then you could plot a heat map of certain types of crimes you choose.')
    print()
    while True:

        print("Choose a level of offense: felony, misdemeanor, violation e.g. 'felony'; 'quit' to return: ", end='')
        level = input('')
        if level.upper() == 'QUIT':
            break

        query = """
        select count(cmplnt_num) as count, ofns_desc from
        complaint
        natural join
        offense_cd
        where law = %s
        group by ofns_desc
        order by count desc
        limit 15
        """
        arg = [level.upper()]
        cursor.execute(query, arg)
        crimes = cursor.fetchall()
        if crimes == []:
            print('Record not found! Make sure you type the right level of offense.')
            print()
            continue

        col_names = []
        for name in cursor.description:
            col_names.append(name[0])
        df = pd.DataFrame(crimes, columns=col_names)
        df.index += 1
        print(df)
        print()

        while True:
            invalid = False
            print("Enter the id of the crimes you want to plot e.g. '1' for the first one.")
            print("You can input multiple ids, separated with ',' e.g. '1,2,6': ", end='')
            plot = input('')
            choice = plot.split(',')
            if plot.replace(',', '').isdigit() is False:
                print('Invalid input! Please try again.')
                print()
                continue
            else:
                pass

            for c in choice:
                if int(c) > 15 or int(c) < 1:
                    invalid = True
                    break

            if invalid is True:
                print('Invalid input! Please try again.')
                print()
                continue
            else:
                break

        ofns_desc = []

        for c in choice:
            try:
                ofns_desc.append(crimes[int(c)-1][1])
            except IndexError:
                print('error!')

        string = ""
        for ofns in ofns_desc:
            string = string + "'%s'," % ofns
        modify = string.rstrip(',')
        print('plot ' + modify)

        # MongoDB quuery
        lat = []
        long = []
        locations = mongo.find({'ofns_desc': {'$in': ofns_desc}},
                               {'lat': 1, 'long': 1})
        for location in locations:
            if location['lat'] is None:
                continue
            lat.append(location['lat'])
            long.append(location['long'])

        print(str(len(long)) + ' records.')
        print()

        # Plot the point to google map.
        gmap = gmplot.GoogleMapPlotter(lat[0], long[0], 10)
        gmap.heatmap(lat, long)
        gmap.draw("heat_map.html")
        print("Heat map saved as 'heat_map.html', please check it manually.")
        print()


if __name__ == '__main__':

    while True:
        
        # valid password for postgres is a must have.
        print('Please enter the password of user postgres')
        print('Password: ', end='')
        _password = input('')
        print()
        try:
            sql_cursor, mongo_client = connect('postgres', _password)
            print('Welcome to database!')
            print()
            break
        except psycopg2.OperationalError:
            print('Invalid password for postgres! Please try again.')
            print()
    functions = ['Initialize databases',
                 'Public safety by borough',
                 'Effect of collision contributor',
                 'From crime occurrence to report',
                 'Top 5 offenses in a specific time period',
                 'Heat map of crimes',
                 'Quit']
    col_name = ['Functions']
    table = pd.DataFrame(functions, columns=col_name)
    table.index += 1
    while True:
        print('We provide the following functions: ')
        print(table)
        print()
        print("Please enter your choice e.g. '1' for initializing, '7' for quit: ", end='')
        choose = input('')

        if choose == '2':
            summary(sql_cursor)

        elif choose == '3':
            mostvtype(sql_cursor)

        elif choose == '4':
            law(sql_cursor)

        elif choose == '5':
            crimedifferenttime(sql_cursor)

        elif choose == '6':
            heatmap(sql_cursor, mongo_client)

        elif choose == '1':
            print('Are you sure to go the whole process? Initialization will take 6-7 mins.')
            print("Enter 'Y' to continue, otherwise return: ", end='')
            decide = input('')
            if decide.upper() == 'Y':
                load_data(sql_cursor, mongo_client)
            else:
                pass

        elif choose == '7':
            print()
            print('Thank you! Good bye!')
            break

        else:
            print('Invalid input, try again!')
            print()
