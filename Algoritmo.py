
from datetime import date
import pandas as pd
import requests
import json

from api_handler import APIBMEHandler
import numpy as np
import utils


get_ticker_master
get_close_data_ticker

def gen_alloc_data(ticker, alloc):
    return {'ticker': ticker,
            'alloc': alloc}



class Algo:
    
    def __init__(self, algo_tag, market='IBEX', produccion = False, primera_ejecucion = False, ventana_datos = 60, n_allocations = 4):
        self.api_handler = APIBMEHandler(market=market, algo_tag=algo_tag)
        self.market = market
        self.algo_tag = algo_tag     
        self.rebal_freq = rebal_freq 
        self.data_close = None
        self.produccion = produccion
        self.BaseDatos = self.algo_tag + "_" + self.market
        self.ultima_seleccion = None
        self.primera_ejecucion = primera_ejecucion
        self.databaseURL = "https://algoritmosinversion-default-rtdb.firebaseio.com/"
        self.ventana_datos = ventana_datos
        self.n_allocations = n_allocations #numero máximo de allocations

        
    def dame_algos(self):
        self.api_handler.get_user_algos()
        
        
    
    def insert_allocs(self):         
        if (self.produccion == False):
            data_total = self.data_close
            data_rebalance = data_total.iloc[::self.rebal_freq]
            ultimo_rebalanceo = data_rebalance.iloc[-1]
            data_rebalance = data_rebalance['2019/01/01':ultimo_rebalanceo.name]
        
            fecha_ant = data_total.iloc[0].name
            for fecha, prices_date in data_rebalance.iterrows():  
                if (fecha_ant != fecha):
                    prices_date= prices_date.dropna()
                    df = pd.DataFrame({
                        tck: self.api_handler.get_close_data_ticker(tck)
                        for tck in prices_date.index.to_list()
                    })
                    df = df[fecha_ant:fecha]
                    fecha_ant = fecha
                    root = hrp(prices=df)

                    weights = pd.DataFrame(root.weights)
                    weights= weights.sort_values('Weights',ascending=False)

                    allocations =op.optimizacion_sharpe(weights, df, self.n_allocations)

                    alloc_list = [gen_alloc_data(allocations.tck[i],allocations.alloc[i]) for i in allocations.index]
                    
                    self.api_handler.post_alloc(date.today().strftime('%Y-%m-%d'), alloc_list)
        else: ##estamos en produccion
            logger.info("Inicio proceso " + "algoritmo " + self.BaseDatos) 
            try:
                #data_total = self.api_handler.get_close_data()
                data_total = self.data_close
                if type(data_total) is None:
                    logger.info("No he podido bajar datos de BME " + "algoritmo " + self.BaseDatos)
                else:
                    data_today = data_total.iloc[-1] #datos de hoy para ver que tickers tenemos
                    data_rebalance = data_total.iloc[::self.rebal_freq]
                    rebalanceo_ant = data_rebalance.iloc[-2]
                    ultimo_rebalanceo = data_rebalance.iloc[-1]

                    datos = data_today.name.strftime('%Y-%m-%d')
                    filtro = ultimo_rebalanceo.name.strftime('%Y-%m-%d')

                    if (self.ultima_seleccion is None):
                        F_ult_seleccion = "" 
                    else:
                        F_ult_seleccion = self.ultima_seleccion 

                    logger.info("Fecha datos " + datos + " filtro rebalanceo " + filtro + " F_ult_seleccion " + F_ult_seleccion) 
                    print("Fecha datos " + datos + " filtro rebalanceo " + filtro + " F_ult_seleccion " + F_ult_seleccion)
                    if (datos == filtro) or (filtro > F_ult_seleccion) or (self.ultima_seleccion is None): ##hoy toca rebalancear todo

                        logger.info("Toca rebalanceo " + "algoritmo " + self.BaseDatos)
                        #data_total = data_total[rebalanceo_ant:ultimo_rebalance] #cojo solo los datos del periodo de rebalanceo
                        df = data_total[data_today.index.to_list()] #cojo solo los tickers que hoy estan activos
                        df = df.tail(self.ventana_datos) 
                        
                        #borramos aquellos tikers que no tengan suficiente historico
                        tickers = df.columns
                        for ticker in tickers:
                            if (df.loc[:,ticker].dropna().shape[0] < (self.ventana_datos)):
                                #elimino el ticker del data frame                                
                                if df.loc[:,ticker].dropna().shape[0] > 0:
                                    logger.info("######### borro el ticker por tener pocos datos historicos .... " + ticker)
                                df = df.drop([ticker],axis = 1)
                        
                        df = df.dropna(axis=1, how='all', thresh=None, subset=None, inplace=False)
                        
                        root = hrp(prices=df)
                        weights = pd.DataFrame(root.weights)
                        weights= weights.sort_values('Weights',ascending=False)
                        
                        allocations =op.optimizacion_sharpe(weights, df, self.n_allocations)
                        alloc_list = [gen_alloc_data(allocations.tck[i],allocations.alloc[i]) for i in allocations.index]
                        
                        logger.info("Tengo la selección y los pesos" + "algoritmo " + self.BaseDatos)
                        
                        str_date = date.today().strftime('%Y-%m-%d')
                        logger.info("Pesos asignados " + "algoritmo " + self.BaseDatos)
                        self.api_handler.post_alloc(date.today().strftime('%Y-%m-%d'), alloc_list)
                        self.ultima_seleccion = data_today.name.strftime('%Y-%m-%d')
                        logger.info("Pesos comunicados " + "algoritmo " + self.BaseDatos)
                        self.datos_guarda_situacion(alloc_list,str_date)
                    else:
                        logger.info("NO Toca rebalanceo " + "algoritmo " + self.BaseDatos)
            except DescargaBME:
                logger.error("Error al descargar y procesar datos BME a las ")
                raise DescargaBME("Error al descargar los datos de BME")
            
    def run_bt(self):
        result = self.api_handler.run_backtest()
        return result
    
    def datos_situacion(self):
        try:
            if (self.primera_ejecucion == True):
                logger.info("Primera ejecución ultima seleccion en blanco algoritmo " + self.BaseDatos)
            else:    
                if not firebase_admin._apps:
                    cred_obj = firebase_admin.credentials.Certificate('algoritmosinversion-firebase-adminsdk-v0kf6-0b0b90de17.json')
                    default_app = firebase_admin.initialize_app(cred_obj, {
                        'databaseURL':self.databaseURL
                    })

                ref = db.reference("/" + self.BaseDatos)
                snapshot = ref.order_by_key().get()
                fecha, datos = snapshot.popitem() 
                #snapshot es un OrderedDict
                self.ultima_seleccion = fecha
                logger.info("Recuperada la información de la base de datos ultimo rebalanceo " + fecha + " algoritmo " + self.BaseDatos)
                return(fecha, datos)   
        except ErrorConectarBaseDatos:            
            raise ErrorConectarBaseDatos("Error al conectar o recuperar info de la base de datos igual hay que establecer self.primeraejecucion")
        
    def datos_guarda_situacion(self,alloc_list, str_date):        
        try:
            if not firebase_admin._apps:
                cred_obj = firebase_admin.credentials.Certificate('algoritmosinversion-firebase-adminsdk-v0kf6-0b0b90de17.json')
                default_app = firebase_admin.initialize_app(cred_obj, {
                    'databaseURL':self.databaseURL
                })
            #if self.primera_ejecucion ==True:
            #primera ejecucion del algoritmo
                 #logger.info("Creo la estructura de la base de datos tras primera ejecución " + "algoritmo " + self.BaseDatos)
                 #ref = db.reference("/")
                 #ref.set({                    
                  #   self.BaseDatos: -1                    
                 #})

            ref = db.reference("/" + self.BaseDatos)
            ref.set({
                str_date:{
                    "alloc": json.dumps(alloc_list),
                    "fecha": str_date
                }
            })
            
            logger.info("He guardado en la base de datos " + "algoritmo " + self.BaseDatos)
        except ErrorAlGuardarBaseDatos:
            raise ErrorAlGuardarBaseDatos("Error al guardar los datos en la base de datos")
    def run_algo(self):
        logger.info("Inicio en producción a las " + "algoritmo " + self.BaseDatos) 
        if (self.produccion == False or self.primera_ejecucion == True):
            self.api_handler.delete_allocs()
        self.datos_situacion()
        
        self.insert_allocs()
        if (self.produccion == False):
            self.run_bt()   
