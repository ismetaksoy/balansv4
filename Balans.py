import sqlite3
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import yfinance as yf
import streamlit as st
from datetime import datetime
import altair as alt
import os
import investpy

# Vaste perioden bepalen
# bij start moet hij zoeken naar de startwaarde van die dag
# bij end moet hij zoeken naar de eindwaarde van die dag
periode = {
    'Q1':
    {'start':'2020-01-02',
    'end':'2020-03-31'},
    'Q2':
    {'start':'2020-04-01',
    'end':'2020-06-30'},
    'Q3':
    {'start':'2020-07-01',  
    'end':'2020-09-30'},
    'Q4':
    {'start':'2020-10-01',
    'end':'2020-12-31'}
}


def LoadData():
    
    # De input bestanden worden geleverd door Binck zonder header. Deze voegen wij handmatig toe aan ieder bestand

    posreconhead = ['Account_Number', 'Datum', 'Symbol', 'ISIN_Code', 'Derivative_Type', 'Expiration_Date', 'Exercise_Price', 'Position_Currency', 'Market_Price', 'Amount_or_Quantity', 'Exchange_Rate', 'Contract_Size', 'Current_Value_in_EUR', 'Current_Value_in_Position_Currency', 'Historic_Value', 'Instrument_Type_Description', 'Binck_ID', 'Instrument_Name', 'Accrued_Interest']
    

    tradereconhead = ['Account_Number', 'Account_Type', 'Account_Currency', 'Datum', 'Transaction_Time', 'Reverse_Transaction', 'Transaction_Type_Code', 'Exchange_Rate', 'Transaction_Currency', 'Quantity', 'Price', 'Invoice_Amount', 'Brokerage_Fees', 'Taks', 'Interest', 'Value_Date', 'Transaction_Number', 'ISIN_Code', 'Symbol', 'Subtype_optie', 'Expiration_Date', 'Expiration_Price', 'Instrument_Type_Code', 'Undefined_1', 'Undefined_2', 'Undefined_3', 'Undefined_4', 'Book_Date', 'Instrument_Type_Description', 'Binck_ID', 'Instrument_Name', 'Deposit_Value', 'Exchange_Code', 'Other_Transaction_Costs', 'Reference_Code', 'Market_costs', 'Line_number', 'Long/Short_Indicator', 'FX_cost', 'FX_gross_costs', 'FX_net_costs', 'Storno_transaction']
        
        
    # Maak connectie met de database en geef de locaties aan van de input bestanden
    posdirectory = './Input/Posrecon'
    tradedirectory = './Input/Traderecon'
    conn = sqlite3.connect('DatabaseVB.db')
    
    # Loop over de input bestanden
    # laad ze in de database
    # verplaats de bestanden naar de respectievelijke archief map
    for file in os.listdir(posdirectory):
        df = pd.read_csv(posdirectory+'/'+file, names = posreconhead, delimiter = ';', decimal = ',', parse_dates = True)
        df.to_sql('Posrecon', if_exists = "append", con = conn)
        os.rename(posdirectory+'/'+file , './ArchivePosrecon/'+file)

    for file in os.listdir(tradedirectory):
        df = pd.read_csv(tradedirectory+'/'+file, names = tradereconhead, delimiter = ';', decimal = ',', parse_dates = True)
        df.to_sql('Traderecon', if_exists = "append", con = conn)
        os.rename(tradedirectory+'/'+file , './ArchiveTraderecon/'+file)

    
    # Moederfunctie.
    # Deze functie haalt de volledige tabel op voor een rekening nummer uit de database en returnt een dataframe met alle waardes. 
    # Deze dataframe gebruiken dan voor de overige functies om zo bepaalde waardes eruit te vissen


def GetRendement(x):
    ### 1: creeeren van brug tussen sql query en database
    ### 2a: bepaal de eindwaarde per dag voor klant x
    ### 2b: bepaal de stortingen per dag voor klant x
    ### 2c: bepaal de deponeringen per dag voor klant x
    ### 2d: bepaal de onttrekkingen per dag voor klant x
    ### 2e: bepaal de lichtingen per dag voor klant x
     
    
    ### 1: creeeren van brug tussen sql query en database
    engine = create_engine('sqlite:///DatabaseVB.db')
    
    
    ### 2a: bepaal de eindwaarde per dag voor klant x (datum, eindwaarde)
    df_posrecon = pd.read_sql(f'''SELECT "Datum", ROUND(sum("Current_Value_in_EUR"),2) as "Eind Waarde" FROM Posrecon WHERE "Account_Number" = "{x}" group by "Datum" order by "Datum"''', con = engine).set_index('Datum')
    
    
    ### 2b: bepaal de stortingen per dag voor klant x (datum, stortingen)
    ### (som van kolom invoice amount, indien OF (Transaction Type Code=O-G en Reference Code=5026) OF (Transaction Type Code=O-G en Reference Code=5000 en kolom invoice amount >0)
    df_stortingen = pd.read_sql (f'''  SELECT "Datum", sum("Invoice_Amount") as "Stortingen" FROM Traderecon WHERE "Account_Number" = "{x}"  AND "Reference_Code" = 5026 OR ("Account_Number" = "{x}" AND "Reference_Code" = 5000 AND "Invoice_Amount" > 0) group by "Datum" order by "Datum" ''', con = engine).set_index('Datum')
    
    
    ### 2c: bepaal de deponeringen per dag voor klant x (datum, deponeringen)
    ### som van kolom Deposit Value, indien (1) Transaction Type Code = D, of (2) Transaction Type Code = O en Deposit value > 0. 
    
    df_deponeringen = pd.read_sql (f''' SELECT "Datum", sum("Deposit_Value") as Deponeringen FROM Traderecon WHERE ("Account_Number" = "{x}" AND "Transaction_Type_Code" = "D") OR ("Account_Number" = "{x}" AND "Transaction_Type_Code" = "O" AND "Deposit_Value" > 0) group by "Datum" order by "Datum" ''', con = engine).set_index('Datum')
    
    
    ### 2d: bepaal de onttrekkingen per dag voor klant x (datum, onttrekkingen)
    ### (som van kolom invoice amount *-1, indien (1) Reference Code=5025, (2) Reference Code=5000 en invoice amount < 0.   
    df_onttrekking = pd.read_sql (f''' SELECT Datum, sum("Invoice_Amount")*-1 as "Onttrekkingen" FROM Traderecon WHERE ("Account_Number" = "{x}" AND "Reference_Code" = 5025) OR ("Account_Number" = "{x}" AND "Reference_Code" = 5000 AND "Invoice_Amount" < 0) group by "Datum" order by "Datum" ''', con = engine).set_index('Datum')
    

    ### 2e: bepaal de lichtingen per dag voor klant x (datum, lichtingen)
    ### som van kolom Deposit Value *-1, indien (1) Transaction Type Code = L, of (2) Transaction Type Code = O en Deposit value < 0. 
    df_lichtingen = pd.read_sql (f''' SELECT Datum, sum("Deposit_Value")*-1 as "Lichtingen"  FROM Traderecon WHERE ("Account_Number" = "{x}" AND "Transaction_Type_Code" = "L") OR ("Account_Number" = "{x}" AND "Transaction_Type_Code" = "O" AND "Deposit_Value" < 0) group by "Datum" order by "Datum" ''', con = engine).set_index('Datum')


    # Concat de 4 dataframes uit de Traderecon query in 1 dataframe en merge deze met de Posrecon dataframe
    traderecon_data = [df_onttrekking, df_stortingen, df_lichtingen, df_deponeringen]
    df_tot_tr = pd.concat(traderecon_data).fillna(0).groupby(['Datum']).sum()
    df_final = df_posrecon.merge(df_tot_tr, on='Datum', how='outer')
    
    ### VOEG DE OVERBOEKINGEN AAN DE DATAFRAME MET DE WAARDES PORTEFEUILLE
    traderecon_columns = ['Onttrekkingen', 'Stortingen', 'Lichtingen', 'Deponeringen']
    df_final[traderecon_columns] = df_final[traderecon_columns].fillna(0.0)
    
    
    ### MAAK KOLOM ACTUELE RENDEMENT EN BEREKEN RENDEMENT VAN WAARDE PORTEFEUILLE EN ONTTREKKINGEN / STORTINGEN
    # start waarde is de eind waarde van de vorige dag
    df_final['Start Waarde'] = df_final["Eind Waarde"].shift(1)
    #df_final['Start Waarde'] = df_final["Start Waarde"].fillna(df_final["Eind Waarde"])
    df_final['Eind Waarde'] = df_final['Eind Waarde'].fillna(df_final['Start Waarde'] + df_final['Stortingen'] + 
                                                             df_final['Deponeringen'] - df_final['Onttrekkingen'] - 
                                                             df_final['Lichtingen'])

    df_final['Dag Rendement'] = ((df_final['Eind Waarde'] - df_final['Start Waarde'] - df_final['Stortingen'] - df_final['Deponeringen'] + df_final['Onttrekkingen'] + df_final['Lichtingen'] ) ) / (df_final['Start Waarde'] + df_final['Stortingen'] + df_final['Deponeringen'] - df_final['Onttrekkingen'] - df_final['Lichtingen']).round(5)
    df_final['Dag Rendement'] = df_final['Dag Rendement'].fillna(0)  
    
    
    df_final['EW Portfolio Cumulatief Rendement'] = (1 + df_final['Dag Rendement']).cumprod()

    df_final['SW Portfolio Cumulatief Rendement'] = df_final['EW Portfolio Cumulatief Rendement'].shift(1)
    df_final['SW Portfolio Cumulatief Rendement'] = df_final['SW Portfolio Cumulatief Rendement'].fillna(1)
    #df_final['SW Portfolio Cumulatief Rendement'] = df_final['SW Portfolio Cumulatief Rendement'].fillna(1.0)
    #df_final['Eind Waarde'] =  pd.to_numeric(df_final['Eind Waarde'], downcast = 'float')
    columns = ['Start Waarde','Stortingen','Deponeringen', 'Onttrekkingen', 'Lichtingen', 'Eind Waarde', 'Dag Rendement', 'SW Portfolio Cumulatief Rendement', 'EW Portfolio Cumulatief Rendement']
    
    return df_final[columns]


# Overview portefeuille Ontwikkeling
# Deze functie wordt gebruikt om per kwartaal te laten zien wat de portefeuille heeft gepresteerd

@st.cache
def GetOverview(data, kwartaals): 
    startwaarde, stortingen, deponeringen, onttrekkingen, lichtingen, eindwaarde, startcumrendement, eindcumrendement = [],[],[],[],[],[],[],[]
    for kwartaal in kwartaals:
        startwaarde.append(data.loc[periode[kwartaal]['start'],['Start Waarde']][0])
        stortingen.append((data.loc[periode[kwartaal]['start']:periode[kwartaal]['end'],['Stortingen']]).sum()[0])
        deponeringen.append((data.loc[periode[kwartaal]['start']:periode[kwartaal]['end'],['Deponeringen']]).sum()[0])
        onttrekkingen.append((data.loc[periode[kwartaal]['start']:periode[kwartaal]['end'],['Onttrekkingen']]).sum()[0])
        lichtingen.append((data.loc[periode[kwartaal]['start']:periode[kwartaal]['end'],['Lichtingen']]).sum()[0])
        eindwaarde.append(data.loc[periode[kwartaal]['end'],['Eind Waarde']][0])
        startcumrendement.append(data.loc[periode[kwartaal]['start'],['SW Portfolio Cumulatief Rendement']][0])
        eindcumrendement.append(data.loc[periode[kwartaal]['end'],['EW Portfolio Cumulatief Rendement']][0])


    overview = list(zip(startwaarde, stortingen, deponeringen, onttrekkingen, lichtingen, eindwaarde, startcumrendement, eindcumrendement))
    
    df = pd.DataFrame(overview, 
           columns=["Start Waarde", "Stortingen", "Deponeringen", "Onttrekkingen", "Lichtingen", "Eind Waarde",  "SW Portf Cum Rend", "EW Portf Cum Rend"], index = kwartaals)

    df['Abs Rendement'] = df['Eind Waarde'] - df['Start Waarde'] - df['Stortingen'] - df['Deponeringen'] + df['Onttrekkingen'] + df['Lichtingen']

    df['Periode Cum Rendement'] = (df['EW Portf Cum Rend'] - df['SW Portf Cum Rend']) / df['SW Portf Cum Rend']

    return df


# Full Benchmark data
@st.cache(allow_output_mutation=True)
def getBenchmarkData(bench):
    conn = sqlite3.connect('DatabaseVB.db')
    engine = create_engine('sqlite:///DatabaseVB.db')

    ticker = yf.Ticker(bench)
    df_benchmark = ticker.history(period='20y')

    df_benchmark.reset_index(inplace = True)
    df_benchmark.rename(columns = {'Date':'Datum', 'Close': 'Eind Waarde'}, inplace = True)
    df_benchmark['Start Waarde'] = df_benchmark['Eind Waarde'].shift(1)
    df_benchmark['Benchmark Dag Rendement'] = ((df_benchmark['Eind Waarde'] - df_benchmark['Start Waarde']) / df_benchmark['Start Waarde']).round(5)
    df_benchmark['Benchmark Dag Rendement'] = df_benchmark['Benchmark Dag Rendement'].fillna(0)
    df_benchmark.to_sql(f'{bench}', if_exists = 'replace', con = conn)

    df = pd.read_sql(f'''
        SELECT substr(Datum, 1, 10) as "Datum", "Start Waarde", "Eind Waarde", "Benchmark Dag Rendement" FROM "{bench}"
    ''', con = engine).set_index("Datum")
    return df


# Overview Benchmark Ontwikkeling
@st.cache
def getPerf(data, kwartaals, bench):
    kwart, startwaarde, eindwaarde = [], [], []
    for kwartaal in kwartaals:
        kwart.append(kwartaal)
        startwaarde.append(data.loc[periode[kwartaal]['start']][0])
        eindwaarde.append(data.loc[periode[kwartaal]['end']][0])

        overview = list(zip(kwart, startwaarde, eindwaarde))

        df = pd.DataFrame(overview, columns=['Kwartaal','Start Waarde','Eind Waarde'],
                         index = kwart)
        
        df['Benchmark Performance'] = (df['Eind Waarde'] - df['Start Waarde']) / df['Start Waarde']     
    return df


def Graph(data, benchmark, ticker, period):
    sorted_periode = sorted(period)

    # benchmark['Start Waarde'] = benchmark[f'{ticker} Eind Waarde'].shift(1)
    # benchmark['Benchmark Dag Rendement'] = ((benchmark[f'{ticker} Eind Waarde'] - benchmark['Start Waarde']) / benchmark['Start Waarde']).round(5)

    df_port_bench = data.merge(benchmark, on='Datum', how='left')
    df_port_bench['Benchmark Dag Rendement'].fillna(0)
    df_port_bench['Benchmark Cumulatief Rendement'] = (1 + df_port_bench['Benchmark Dag Rendement']).cumprod()
    df_port_bench['Benchmark Cumulatief Rendement'].fillna(method='ffill', inplace = True)
    df_base = df_port_bench[['EW Portfolio Cumulatief Rendement', 'Benchmark Cumulatief Rendement']]
    df_base.rename(columns = {'EW Portfolio Cumulatief Rendement':'Portefeuille', 'Benchmark Cumulatief Rendement':'Benchmark' }, inplace = True)

    if len(period) > 1:
        start = periode[sorted_periode[0]]['start']
        end = periode[sorted_periode[-1]]['end']
    else:
        start = periode[sorted_periode[0]]['start']
        end = periode[sorted_periode[0]]['end']

    df = df_base.loc[start:end]

    dfn = df.reset_index().melt('Datum')
    dfn1 = alt.Chart(dfn).mark_line().encode(
        x = ('Datum:T'),
        y = ('value:Q'),
        color='variable:N').properties(
            height=500,
            width=750).interactive()

    graph = st.altair_chart(dfn1) 

    return graph



# Handmatig kiezen van start- en einddatum voor de portefeuille ontwikkeling
# We laden hierbij de dataframe van functie GetRendement in en gaan deze slicen op de start date en end date
# Dan halen we de startwaarde, eindwaarde deponeringen etc op uit die dataframe door middel van loc functie.
# Dit zijn single value variabelen die we dan in een one row dataframe zetten.
@st.cache(allow_output_mutation=True)
def ZoekPortfOntwikkeling(data, sd, ed):

    df = data.loc[sd:ed]
    portf_startwaarde = df.loc[sd,['Start Waarde']][0]
    portf_stortingen = df.loc[sd:ed,['Stortingen']].sum()[0]
    portf_deponeringen = df.loc[sd:ed,['Deponeringen']].sum()[0]
    portf_onttrekkingen = df.loc[sd:ed,['Onttrekkingen']].sum()[0]
    portf_lichtingen = df.loc[sd:ed,['Lichtingen']].sum()[0]
    portf_eindwaarde = df.loc[ed,['Eind Waarde']][0]
    portf_startcumrendement = df.loc[sd,['SW Portfolio Cumulatief Rendement']][0]
    portf_eindcumrendement = df.loc[ed,['EW Portfolio Cumulatief Rendement']][0]
    portf_absrendement = portf_eindwaarde - portf_startwaarde - portf_stortingen - portf_deponeringen + portf_onttrekkingen + portf_lichtingen
    portf_cumrendement = (portf_eindcumrendement - portf_startcumrendement) / portf_startcumrendement


    overview = ['{:,.2f}'.format(portf_startwaarde), '{:,.2f}'.format(portf_stortingen), '{:,.2f}'.format(portf_deponeringen), 
    '{:,.2f}'.format(portf_onttrekkingen), '{:,.2f}'.format(portf_lichtingen),'{:,.2f}'.format(portf_eindwaarde), '{:.2%}'.format(portf_startcumrendement), '{:.2%}'.format(portf_eindcumrendement), '{:,.2f}'.format(portf_absrendement), '{:.2%}'.format(portf_cumrendement)]

    df_final = pd.DataFrame([overview], columns = ['Start Waarde', 'Stortingen', 'Deponeringen', 'Onttrekkingen', 'Lichtingen', 'Eind Waarde', 'Start Cum Rend', 'Eind Cum Rend', 'Abs Rendement', 'Periode Cum Rendement'])
    return df_final

@st.cache(allow_output_mutation=True)
def ZoekBenchmarkOntwikkeling(df, bench, start_date, end_date):
    df_klant = df.reset_index()
    df_klant = df_klant[['Datum']]
    bench = df_klant.merge(bench, how='left', on='Datum')#.groupby(['Datum']).mean()
    bench = bench.set_index('Datum')
    bench['Eind Waarde'] = bench['Eind Waarde'].fillna(method='ffill')
    bench["Start Waarde"] = bench["Eind Waarde"].shift(1)
    bench['Benchmark Dag Rendement'] = ((bench['Eind Waarde'] - bench['Start Waarde']) / bench['Start Waarde']).round(5)
    bench['Benchmark Dag Rendement'] = bench['Benchmark Dag Rendement'].fillna(0)

    new_benchmark_df = bench[start_date:end_date]
    bench_sw = new_benchmark_df.loc[start_date,["Start Waarde"]][0]
    bench_ew = new_benchmark_df.loc[end_date,["Eind Waarde"]][0]
    
    
    overview = ['{:.2f}'.format(bench_sw), '{:.2f}'.format(bench_ew)]
    df = pd.DataFrame([overview], columns =['Start Waarde', 'Eind Waarde'])
    
    df['Abs Rendement'] = '{:.2f}'.format(bench_ew - bench_sw)
    
    df['Rendement'] = '{:.2%}'.format((bench_ew - bench_sw) / bench_sw)
    
    return df


#Functie voor de grafiek met als start en eind datum handmatige selectie
def ZoekGraph(data, benchmark,  start_date, end_date):
    df_port_bench = data.merge(benchmark, on='Datum', how='left')
    df_port_bench['Benchmark Dag Rendement'].fillna(0)
    df_port_bench['Benchmark Cumulatief Rendement'] = (1 + df_port_bench['Benchmark Dag Rendement']).cumprod()
    df_port_bench['Benchmark Cumulatief Rendement'].fillna(method='ffill', inplace = True)
    df_base = df_port_bench[['EW Portfolio Cumulatief Rendement', 'Benchmark Cumulatief Rendement']]
    df_base.rename(columns = {'EW Portfolio Cumulatief Rendement':'Portefeuille', 'Benchmark Cumulatief Rendement':'Benchmark' }, inplace = True)
    df = df_base.loc[start_date:end_date]

    dfn = df.reset_index().melt('Datum')
    dfn1 = alt.Chart(dfn).mark_line().encode(
        x = ('Datum:T'),
        y = ('value:Q'),
        color = 'variable:N',
        tooltip=['Datum:T','value:Q']).properties(
            height = 500,
            width = 750).interactive()


    graph = st.altair_chart(dfn1) 

    return graph
                 

#Portefeuille weergave per einddatum
def ShowPortfolio(x, date):
    engine = create_engine('sqlite:///DatabaseVB.db')
    df = pd.read_sql(f""" SELECT "Datum", "Amount_or_Quantity" as "Aantal","Instrument_Name" as "Instrument", "Market_Price" as "Slotkoers", "Current_Value_in_EUR" as "Waarde in EUR","Symbol" as "Symbool" FROM Posrecon WHERE "Account_Number" = "{x}" AND "Datum" = "{date}" order by "Current_Value_in_EUR" DESC """, con = engine).set_index('Datum')
    return df

#Transactieoverzicht gedurende periode
def ShowTransaction(x):
    engine = create_engine('sqlite:///DatabaseVB.db')
    df = pd.read_sql(f"""  SELECT "Datum", "Transaction_Type_Code" as "Type transactie", "Transaction_Currency" as "Transactie valuta", "Quantity" as "Aantal", "Instrument_Name" as "Instrument", "Price" as "Koers", "Invoice_Amount" as "Totaal bedrag", "Brokerage_Fees" as "Kosten Broker", "Other_Transaction_Costs" as "Overige kosten","Reference_Code" as "Referentie code" FROM Traderecon WHERE "Account_Number" = "{x}"  order by "Datum" """, con = engine).set_index('Datum')
    return df
    
#Indien je de users in dropdownlist wil, zet onderste regels aan.    
#@st.cache()
def Users():
   engine = create_engine('sqlite:///DatabaseVB.db')
   df = pd.read_sql('''Select distinct(Account_Number) as Users from Posrecon order by Users asc;''', con = engine) 
   return df['Users']


# Ophalen van benchmark uit investing
@st.cache(allow_output_mutation=True)
def BenchmarkDataInvesting(bench, country):
    conn = sqlite3.connect('DatabaseVB.db')
    df = investpy.get_index_historical_data(index=f'{bench}',
                                          country=f'{country}',
                                          from_date='01/01/2000',
                                          to_date= datetime.today().strftime("%d/%m/%Y"))
    df.reset_index(inplace = True)
    df.rename(columns ={'Date':'Datum'}, inplace = True)
    df.to_sql(f'{bench}', if_exists = "replace", con = conn)
    #df_benchmark = pd.read_sql(f''' SELECT substr(datum,1,10) as Datum, close as "Eind Waarde" FROM "{bench}" ''',
    #                         con = conn).set_index('Datum')
    #return df_benchmark

# Ophalen van start datum klantendatabase en mergen met de benchmarks
@st.cache()
def KlantData(data, bench):

    klantdatum = data.reset_index()
    klantdatum = klantdatum[['Datum']]
    df = klantdatum.merge(bench, how = 'left', on = 'Datum' )#.groupby(['Datum']).mean()
    df = df.fillna(method = 'ffill')
    df['Benchmark Dag Rendement'] = ((df['Eind Waarde'] - df['Start Waarde']) / df['Start Waarde']).round(5)
    df['Benchmark Dag Rendement'] = df['Benchmark Dag Rendement'].fillna(0)
    df = df.fillna(method = 'bfill')
    df = df.set_index("Datum")
    return df
# Presenteren benchmark ontwikkeling
@st.cache
def PortfBenchOverzicht(data, start_date, end_date):
    startwaarde = data.loc[start_date,["Start Waarde"]]
    eindwaarde = data.loc[end_date,["Eind Waarde"]]

    data = list(zip(startwaarde, eindwaarde))
    df = pd.DataFrame(data, columns = ["Start Waarde", "Eind Waarde"])
    
    df["Absolute Rendement"] = (df["Eind Waarde"][0] - df["Start Waarde"][0])
    df["Rendement"] = "{:.2%}".format(df["Absolute Rendement"][0] / df["Start Waarde"][0])
    
    return df


# def ZoekGraph(data, bench, start_date, end_date):
#     df_port_bench = data.merge(bench, on="Datum", how="left")
#     df_port_bench['Benchmark Dag Rendement'].fillna(0)
#     df_port_bench['Benchmark Cumulatief Rendement'] = (1 + df_port_bench['Benchmark Dag Rendement']).cumprod()
#     df_port_bench['Benchmark Cumulatief Rendement'].fillna(method='ffill', inplace = True)
#     df_base = df_port_bench[['EW Portfolio Cumulatief Rendement', 'Benchmark Cumulatief Rendement']]
#     df_base = df_base.rename(columns={'EW Portfolio Cumulatief Rendement':'Portfolio', 'Benchmark Cumulatief Rendement': 'Benchmark'})

#     df = df_base.loc[start_date:end_date]
#     dfn = df.reset_index().melt('Datum')
#     dfn1 = alt.Chart(dfn).mark_line().encode(
#         x = ('Datum:T'),
#         y = ('value:Q'),
#         color = 'variable:N').properties(
#             height = 500,
#             width = 750).interactive()

#     graph = st.altair_chart(dfn1) 

#     return graph
#                  

# Rapportage functie met overzicht van alle rekeningnummers en de portefeuille ontwikkeling
# Het systeem kijkt in de database wat de start en eind datums zijn voor de rekeningnummer en geeft deze als output


# Deze functie maakt een list van alle account numbers
def klantenlijst():
    engine = create_engine('sqlite:///DatabaseVB.db')
    klantenlijst = pd.read_sql(f'''
    select distinct(account_number) from posrecon;
    ''', con = engine)
    klantenlist = klantenlijst.iloc[:-1,0]
    return klantenlist

# Deze functie berekent wat de actuele start datum is voor een portfolio (Met actueel bedoelen we de datum dat in de database zit)
def start_date(reknr, start_d):
    engine = create_engine('sqlite:///DatabaseVB.db')
    start_date = pd.read_sql(f'''
        select distinct(datum) from Posrecon where Account_Number = "{reknr}"
        union
        select distinct(datum) from Traderecon where Account_Number = "{reknr}"
        order by datum asc limit 1;
        ''', con = engine)
    final_start_date = pd.to_datetime(start_date['Datum'][0]).strftime("%Y-%m-%d")

    if start_d < final_start_date:
            start_d = final_start_date
    return start_d
    
def end_date(reknr, end_d):
    engine = create_engine('sqlite:///DatabaseVB.db')
    end_date = pd.read_sql(f'''
    select distinct(datum) from Posrecon where Account_Number = "{reknr}"
    union
    select distinct(datum) from Traderecon where Account_Number = "{reknr}"
    order by datum desc limit 1;
    ''', con = engine)
    final_end_date = pd.to_datetime(end_date['Datum'][0]).strftime("%Y-%m-%d")
    
    if end_d > final_end_date:
        end_d = final_end_date
    return end_d

# Deze functie maakt een overzicht van portefeuille ontwikkelingen voor alle klanten
@st.cache(allow_output_mutation=True, suppress_st_warning=True)
def rapport(klanten, start_d, end_d):
    empty = []
    for klant in klanten:
        sd = start_date(klant, start_d)
        ed = end_date(klant, end_d)
        data = GetRendement(klant)
        overzicht = ZoekPortfOntwikkeling(data, sd ,ed)
        overzicht['Account_Number'] = klant
        overzicht['Start_Datum'] = sd
        overzicht['Eind_Datum'] = ed
        overzicht = overzicht.set_index(['Account_Number'])
        empty.append(overzicht.iloc[0])
        df = pd.DataFrame(empty)
    return df

# Deze functie toont de invoice amounts voor alle klanten
def Invoice_amount():
    engine = create_engine('sqlite:///DatabaseVB.db')
    df = pd.read_sql(f'''
    select Account_Number, Invoice_Amount, Datum
    from traderecon
    where Reference_code = 5200
    order by Account_number asc, Datum asc;
    ''', con = engine).set_index('Account_Number')
    return df