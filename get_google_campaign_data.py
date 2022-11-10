import datetime
import os
import pandas as pd
from MySQLdb import OperationalError, IntegrityError
from google.ads.googleads.client import GoogleAdsClient
from dotenv import load_dotenv
from datetime import datetime, timedelta
from google.ads.googleads.errors import GoogleAdsException
import json
from sqlalchemy import engine
from sqlalchemy.exc import IntegrityError
import argparse
import numpy as np
import logging
logging.basicConfig(filename="log.txt", filemode='w', level=logging.DEBUG, format="%(asctime)s %(message)s")
parser = argparse.ArgumentParser()
parser.add_argument('--y', action='store_true', help='A boolean switch')
parser.add_argument('--date', type=str, required=False, help='validation date')
args = parser.parse_args()


load_dotenv()


class google_ads_data:

    def __init__(self, path=os.path.abspath("google_ads.yaml")):
        self.path = path
        self.customer_id = os.getenv('customer_id')
        self.today = datetime.today()
        self.df = pd.DataFrame()
        self.boolean = args.y
        self.date = str


    def args_val(self):
        """ función que valida el input de los argumentos(fecha y booleano) por consola.
            True: si el valor fecha y boolean son digitados,
            False: si el valor fecha y boolean es omitido,
            salida -> date.
        """
        try:
            #validamos si los argumentos son digitados en consola
            # si el valor es True, retorna la fecha deseada por el usuario
            if self.boolean is True:
                self.date = datetime.strptime(args.date, '%Y-%m-%d') + timedelta(1)
                return self.date

            # si el valor es False, retorna la fecha actual
            if self.boolean is False:
                self.date = self.today
                return self.date
        except Exception:
            logging.exception("ocurrió un error al formatear la fecha de entrada!")


    @classmethod
    def drop_inf(cls, df):
        """ función que reemplaza valores infinitos y -infinitos(np.inf, -np.inf) del dataframe
            y los reemplaza por 0.
            variable -> Dataframe,
            salida -> Dataframe.
        """
        try:
            # creamos la lista que almacenara los dataframes con valores -infinitos e infinitos
            values = []
            for col in df.columns:
                # reemplazamos los valores -infinitos e infinitos por valores nulos (NaN)
                values.append(pd.DataFrame(
                    {col: df.loc[:, col].apply(lambda x: 0 if x == np.inf or x == -np.inf else x)}).reset_index(
                    drop=True))
            # retornamos el dataframe final
            df = pd.concat(values, axis=1)
            return df
        except Exception:
            logging.exception("ocurrió un error al realizar la limpieza del dataframe")


    def download_campaign_data(self):
        """ función que genera los valores de las metricas de la campaña asociadas
            al customer id de ketoro.
            entrada -> Fecha,
            entrada -> customer_id,
            salida -> Dataframe.
        """
        # procesamos el valor de la fecha generada por el usuario o se toma la fecha actual
        try:
            today = datetime.strftime(self.date, '%Y-%m-%d')
            diff = datetime.strptime(today, '%Y-%m-%d') - timedelta(1)
            yesterday = datetime.strftime(diff, '%Y-%m-%d')
            Client = GoogleAdsClient.load_from_storage(self.path)
            ga_service = Client.get_service("GoogleAdsService")

            #se genera el query para devolver los valores correspondientes
            # a las metricas de las campañas asociadas a ketoro
            query = f"""
            SELECT
              campaign.name,
              metrics.cost_micros,
              metrics.impressions,
              metrics.clicks,
              metrics.ctr   
                FROM campaign
                    WHERE campaign.status = 'ENABLED'
                    and campaign.name like '%ketoro%'
                    and segments.date = '{yesterday}'
                    ORDER BY campaign.id
            """

            #retornamos los resultados del query y guardamos toda la informacíon de las metricas
            # de las campañas en un dataframe
            list_dict = []
            #tomamos el customer_id del cliente(ketoro)
            clients = json.loads(os.getenv('client_customer_id'))
            for client in clients:
                #aplicamos el query por customer_id del cliente(ketoro)
                stream = ga_service.search_stream(customer_id=client, query=query)
                for batch in stream:
                    #retornamos toda la información de la consulta
                    for row in batch.results:
                        #validamos los resultados y los guardamos en un dataframe
                        if client == clients[0]:
                            app_dict = {"app": "baz"}
                        else:
                            app_dict = {"app": "banco"}
                        row_dict = {"start_date": yesterday, "campaign_id": row.campaign.name, "campaign_name": row.campaign.name,
                                    "objective": "CONVERSIONS", "spend": row.metrics.cost_micros, "impressions": row.metrics.impressions,
                                    "clicks": row.metrics.clicks, "ctr": round(row.metrics.ctr, 5)}
                        row_dict.update(app_dict)
                        list_dict.append(row_dict)
                self.df = pd.DataFrame(list_dict)
            #creamos las metricas faltantes dentro del dataframe
            self.df["spend"] = self.df["spend"].apply(lambda x: round((x / 1000000), 2))
            self.df["cpc"] = round(self.df["spend"] / self.df["clicks"], 5)
            self.df["cpm"] = round(self.df["spend"] / self.df["impressions"] * 1000, 5)
            #organizamos el dataframe
            self.df = self.df[["start_date", "campaign_id", "app",
                               "campaign_name", "objective", "spend", "impressions", "clicks", "cpc", "cpm", "ctr"]]
            #limpiamos el dataframe
            self.df = self.drop_inf(self.df)
        except GoogleAdsException:
            logging.exception("Se genero un error al consultar los datos en la API!")
        else:
            logging.info("Dataframe creado exitosamente!")


    def upload_table(self):
        """ función que genera el query para la creacion de la tabla,
            que almacenara las metricas calculadas.
            entrada -> Dataframe,
            salida -> Tabla de Mysql.
        """
        try:
            #generamos el query para la creacion de la tabla google_ads_data
            #que almacenara las metricas calculadas por las campañas asociadas al cliente (ketoro)
            q1 = """
                    CREATE TABLE IF NOT EXISTS google_ads_data(
                    start_date DATE NOT NULL,
                    campaign_id VARCHAR(100) NOT NULL,
                    app VARCHAR(20) NOT NULL,
                    campaign_name VARCHAR(100) NOT NULL,
                    objective VARCHAR(100) NOT NULL,
                    spend FLOAT NOT NULL,
                    impressions BIGINT NOT NULL,
                    clicks BIGINT NOT NULL,
                    cpc FLOAT NOT NULL,
                    cpm FLOAT NOT NULL,
                    ctr FLOAT NOT NULL,
                    UNIQUE google_ads_idx (start_date, campaign_id));
                """

            #creamos la conexión a la base de datos de mysql
            conn = engine.create_engine(f"mysql://{os.getenv('user')}:{os.getenv('pass')}@localhost:{os.getenv('port')}/{os.getenv('db')}")
            #abrimos la conexión
            with conn.connect() as mysql_conn:
                result = mysql_conn.execute(q1)
                #insertamos el dataframe a la tabla con todos los datos de la metrica
                self.df.to_sql(con=conn, name='google_ads_data', if_exists='append', index=False)
                #cerramos el cursor y la conexión
                result.close()
            conn.dispose()
        except (OperationalError, IntegrityError):
            logging.exception("los datos no pueden ser insertados en la base de datos!")
        else:
            logging.info("Datos insertados exitosamente en la base de datos!")





if __name__ == "__main__":
    campaign_data = google_ads_data()
    campaign_data.args_val()
    campaign_data.download_campaign_data()
    campaign_data.upload_table()
