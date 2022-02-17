import psycopg2
from shapely import geometry as geo
from shapely import wkb
from config import PostgresConfig
from pprint import pprint

class PostgresSql:
    def __init__(self):
        self.client = psycopg2.connect(host=PostgresConfig.HOST
                       , port=PostgresConfig.PORT
                       , database=PostgresConfig.DB_NAME
                       , user=PostgresConfig.USERNAME
                       , password=PostgresConfig.PASSWORD
                       )
        self.cursor = self.client.cursor()

    def clear_geo_test(self):
        sql_string =f"""
        Truncate Table public.geo_test
        """
        self.cursor.execute(sql_string)
        self.client.commit()  # save data

    def insert_geo_test(self, data):
        sql_string =f"""
        INSERT INTO public.geo_test(id,geo_data)

        VALUES (%s,ST_SetSRID(%s::geometry,4326))
        """
        self.cursor.executemany(sql_string, data)
        self.client.commit()  # save data
    
    def get_geo_test(self):
        self.cursor.execute("SELECT * FROM public.geo_test")
        return [ d for d in self.cursor.fetchall()]
    
    def geo_load(self,data):
        return wkb.loads(data, hex=True).wkt 

pg_sql = PostgresSql()
data = [[1,geo.Point(121,24).wkb_hex]
        ,[2,geo.MultiPoint([(121,24),(121,24.1),(121.1,24)]).wkb_hex]
        ,[3,geo.Polygon([[121,24], [121.2,24.5], [121.7,24.6], [120.9,24.1], [122,24.7]]).wkb_hex]
        ,[4,geo.Point(121.5,24).wkb_hex]]
pg_sql.clear_geo_test()
pg_sql.insert_geo_test(data)
result = pg_sql.get_geo_test()
print([pg_sql.geo_load(d[1]) for d in result[:20]])