import sqlite3
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import yfinance as yf
import streamlit as st
from datetime import datetime
import altair as alt
import os


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
    
    #posreconhead = ['RekNr', 'Datum', 'Symbool', 'ISIN', 'Type optie', 'Expiratie', 'Strike', 'Valuta', 'Slotkoers', 'Aantal', 'Valutakoers', 'Contractgrootte', 'Waarde EUR', 'Waarde Orig Valuta', 'Aankoopwaarde', 'Type instrument', 'Binckcode', 'Titel instrument', 'Unnamed: 18']

    posreconhead = ['Account_Number', 'Datum', 'Symbol', 'ISIN_Code', 'Derivative_Type', 'Expiration_Date', 'Exercise_Price', 'Position_Currency', 'Market_Price', 'Amount_or_Quantity', 'Exchange_Rate', 'Contract_Size', 'Current_Value_in_EUR', 'Current_Value_in_Position_Currency', 'Historic_Value', 'Instrument_Type_Description', 'Binck_ID', 'Instrument_Name', 'Accrued_Interest']
    
    
    # tradereconhead = ['RekNr', 'Unnamed: 1', 'Valuta', 'Datum', 'Tijdstip', 'Unnamed: 5', 'Type', 'Unnamed: 7', 'Unnamed: 8', 'Aantal', 'Per aandeel', 'Bedrag', 'Unnamed: 12', 'Unnamed: 13', 'Unnamed: 14', 'Unnamed: 15', 'Unnamed: 16', 'ISIN', 'Symbool', 'Unnamed: 19', 'Unnamed: 20', 'Unnamed: 21', 'Unnamed: 22', 'Omschrijving', 'Unnamed: 24', 'Unnamed: 25', 'Unnamed: 26', 'Unnamed: 27', 'Unnamed: 28', 'Unnamed: 29', 'Omschrijving overzicht', 'Unnamed: 31', 'Unnamed: 32', 'Unnamed: 33', 'Unnamed: 34', 'Unnamed: 35', 'Unnamed: 36', 'Unnamed: 37', 'Unnamed: 38', 'Unnamed: 39', 'Unnamed: 40', 'Unnamed: 41']

    tradereconhead = ['Account_Number', 'Account_Type', 'Account_Currency', 'Datum', 'Transaction_Time', 'Reverse_Transaction', 'Transaction_Type_Code', 'Exchange_Rate', 'Transaction_Currency', 'Quantity', 'Price', 'Invoice_Amount', 'Brokerage_Fees', 'Taks', 'Interest', 'Value_Date', 'Transaction_Number', 'ISIN_Code', 'Symbol', 'Subtype_optie', 'Expiration_Date', 'Expiration_Price', 'Instrument_Type_Code', 'Undefined_1', 'Undefined_2', 'Undefined_3', 'Undefined_4', 'Book_Date', 'Instrument_Type_Description', 'Binck_ID', 'Instrument_Name', 'Deposit_Value', 'Exchange_Code', 'Other_Transaction_Costs', 'Reference_Code', 'Market_costs', 'Line_number', 'Long/Short_Indicator', 'FX_cost', 'FX_gross_costs', 'FX_net_costs', 'Storno_transaction']
        
        
    # Maak connectie met de database en geef de locaties aan van de input bestanden
    posdirectory = './Input/Posrecon'
    tradedirectory = './Input/Traderecon'
    conn = sqlite3.connect('DatabaseVB1.db')
    
    # Loop over de input bestanden
    # laad ze in de database
    # verplaats de bestanden naar de respectievelijke archief map
    for file in os.listdir(posdirectory):
        df = pd.read_csv(posdirectory+'/'+file, names = posreconhead, delimiter = ',', decimal = ',', parse_dates = True)
        df.to_sql('Posrecon', if_exists = "append", con = conn)
        os.rename(posdirectory+'/'+file , './ArchivePosrecon/'+file)

    for file in os.listdir(tradedirectory):
        df = pd.read_csv(tradedirectory+'/'+file, names = tradereconhead, delimiter = ',', decimal = ',', parse_dates = True)
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
    engine = create_engine('sqlite:///DatabaseVB1.db')
    
    
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
    df_final['Eind Waarde'] = df_final['Eind Waarde'].fillna(df_final['Start Waarde'] + df_final['Stortingen'] + 
                                                             df_final['Deponeringen'] - df_final['Onttrekkingen'] - 
                                                             df_final['Lichtingen'])
    df_final['Dag Rendement'] = ((df_final['Eind Waarde'] - df_final['Start Waarde'] - df_final['Stortingen'] - df_final['Deponeringen'] + df_final['Onttrekkingen'] + df_final['Lichtingen'] ) ) / (df_final['Start Waarde'] + df_final['Stortingen'] + df_final['Deponeringen'] - df_final['Onttrekkingen'] - df_final['Lichtingen']).round(5)
    df_final['Dag Rendement'] = df_final['Dag Rendement'].fillna(0)  
    
    
    df_final['EW Portfolio Cumulatief Rendement'] = (1 + df_final['Dag Rendement']).cumprod()

    df_final['SW Portfolio Cumulatief Rendement'] = df_final['EW Portfolio Cumulatief Rendement'].shift(1)
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
    conn = sqlite3.connect('DatabaseVB1.db')
    engine = create_engine('sqlite:///DatabaseVB1.db')

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



# Grafiek van Portfolio en Benchmark

# def Graph(data, benchmark, ticker, period):
#    sorted_periode = sorted(period)
#
#    benchmark['Start Waarde'] = benchmark[f'{ticker} Eind Waarde'].shift(1)
#    benchmark['Benchmark Dag Rendement'] = ((benchmark[f'{ticker} Eind Waarde'] - benchmark['Start Waarde']) / benchmark['Start Waarde']).round(5)
#    
#    df_port_bench = data.merge(benchmark, on='Datum', how='left')
#
#    df_port_bench['Benchmark Cumulatief Rendement'] = (1 + df_port_bench['Benchmark Dag Rendement']).cumprod()
#    df_port_bench['Benchmark Cumulatief Rendement'].fillna(method='ffill', inplace = True)
#    df_base = df_port_bench[['Portfolio Cumulatief Rendement', 'Benchmark Cumulatief Rendement']]
#    
#    if len(period) > 1:
#        start = periode[sorted_periode[0]]['start']
#        end = periode[sorted_periode[-1]]['end']
#    else:
#        start = periode[sorted_periode[0]]['start']
#        end = periode[sorted_periode[0]]['end']
#
#    df = df_base.loc[start:end]
#
#    dfn = df.reset_index().melt('Datum')
#    dfn1 = alt.Chart(dfn).mark_line().encode(
#        x = ('Datum:T'),
#        y = ('value:Q'),
#        color='variable:N').properties(
#            height=500,
#            width=750).interactive()
#
#    graph = st.altair_chart(dfn1) 
#
#    return graph

def Graph(data, benchmark, ticker, period):
    sorted_periode = sorted(period)

    # benchmark['Start Waarde'] = benchmark[f'{ticker} Eind Waarde'].shift(1)
    # benchmark['Benchmark Dag Rendement'] = ((benchmark[f'{ticker} Eind Waarde'] - benchmark['Start Waarde']) / benchmark['Start Waarde']).round(5)

    df_port_bench = data.merge(benchmark, on='Datum', how='left')
    df_port_bench['Benchmark Dag Rendement'].fillna(0)
    df_port_bench['Benchmark Cumulatief Rendement'] = (1 + df_port_bench['Benchmark Dag Rendement']).cumprod()
    df_port_bench['Benchmark Cumulatief Rendement'].fillna(method='ffill', inplace = True)
    df_base = df_port_bench[['EW Portfolio Cumulatief Rendement', 'Benchmark Cumulatief Rendement']]

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
@st.cache
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

@st.cache
def ZoekBenchmarkOntwikkeling(data, start_date, end_date):
    new_benchmark_df = data[start_date:end_date]
    bench_sw = new_benchmark_df.loc[start_date][0]
    bench_ew = new_benchmark_df.loc[end_date][0]
    overview = ['{:.2f}'.format(bench_sw), '{:.2f}'.format(bench_ew)]
    df = pd.DataFrame([overview], columns =['Start Waarde', 'Eind Waarde'])
    
    df['Abs Rendement'] = '{:.2f}'.format(bench_ew - bench_sw)
    
    df['Rendement'] = '{:.2%}'.format((bench_ew - bench_sw) / bench_sw)
    
    return df





#def ZoekGraph(data, benchmark, ticker, start_date, end_date):
#    # benchmark['Start Waarde'] = benchmark[f'{ticker} Eind Waarde'].shift(1)
#    # benchmark['Benchmark Dag Rendement'] = ((benchmark[f'{ticker} Eind Waarde'] - benchmark['Start Waarde']) / benchmark['Start Waarde']).round(5)
#
#    df_port_bench = data.merge(benchmark, on='Datum', how='left')
#
#    df_port_bench['Benchmark Cumulatief Rendement'] = (1 + df_port_bench['Benchmark Dag Rendement']).cumprod()
#    df_port_bench['Benchmark Cumulatief Rendement'].fillna(method='ffill', inplace = True)
#    df_base = df_port_bench[['Portfolio Cumulatief Rendement', 'Benchmark Cumulatief Rendement']]
#
#    df = df_base.loc[start_date:end_date]
#
#    dfn = df.reset_index().melt('Datum')
#    dfn1 = alt.Chart(dfn).mark_line().encode(
#        x = ('Datum:T'),
#        y = ('value:Q'),
#        color='variable:N').properties(
#            height=500,
#            width=750).interactive()
#
#    graph = st.altair_chart(dfn1) 
#
#    return graph

def ZoekGraph(data, benchmark, ticker, start_date, end_date):
    df_port_bench = data.merge(benchmark, on='Datum', how='left')
    df_port_bench['Benchmark Dag Rendement'].fillna(0)
    df_port_bench['Benchmark Cumulatief Rendement'] = (1 + df_port_bench['Benchmark Dag Rendement']).cumprod()
    df_port_bench['Benchmark Cumulatief Rendement'].fillna(method='ffill', inplace = True)
    df_base = df_port_bench[['EW Portfolio Cumulatief Rendement', 'Benchmark Cumulatief Rendement']]

    df = df_base.loc[start_date:end_date]

    dfn = df.reset_index().melt('Datum')
    dfn1 = alt.Chart(dfn).mark_line().encode(
        x = ('Datum:T'),
        y = ('value:Q'),
        color = 'variable:N').properties(
            height = 500,
            width = 750).interactive()

    graph = st.altair_chart(dfn1) 

    return graph





# Oude zoekportfolio ontwikkeling functie --backup
# def ZoekPortfOntwikkeling1(data, start_datum, eind_datum):
#     sd = start_datum
#     ed = eind_datum

#     df = data.loc[sd:ed]
#     portf_startwaarde = df.loc[sd,['Start Waarde']][0]
#     portf_stortingen = df.loc[sd:ed,['Stortingen']].sum()[0]
#     portf_deponeringen = df.loc[sd:ed,['Deponeringen']].sum()[0]
#     portf_onttrekkingen = df.loc[sd:ed,['Onttrekkingen']].sum()[0]
#     portf_lichtingen = df.loc[sd:ed,['Lichtingen']].sum()[0]
#     portf_eindwaarde = df.loc[ed,['Eind Waarde']][0]
#     startcumrendement = df.loc[sd,['SW Portfolio Cumulatief Rendement']][0]
#     eindcumrendement = df.loc[ed,['EW Portfolio Cumulatief Rendement']][0]
    
    
#     overview = [portf_startwaarde, portf_stortingen, portf_deponeringen, portf_onttrekkingen, portf_lichtingen, 
#                portf_eindwaarde, startcumrendement, eindcumrendement]

#     df_final = pd.DataFrame([overview], columns = ['Start Waarde', 'Stortingen', 'Deponeringen', 'Onttrekkingen', 'Lichtingen', 'Eind Waarde', 'Start Cum Rend', 'Eind Cum Rend'])
    
#     df_final['Abs Rendement'] = df_final['Eind Waarde'] - df_final['Start Waarde'] - df_final['Stortingen'] - df_final['Deponeringen'] + df_final['Onttrekkingen'] + df_final['Lichtingen']
#     #df_final['Rendement'] = (df_final['Abs Rendement'] / df_final['Start Waarde'])
    
#     df_final['Cumulatief Rendement'] = (eindcumrendement - startcumrendement) / startcumrendement
    
#     return df_final

# @st.cache
# def GetOverview(data, kwartaals): 
#     startwaarde, stortingen, deponeringen, onttrekkingen, lichtingen, eindwaarde = [],[],[],[],[],[]
#     for kwartaal in kwartaals:
#         startwaarde.append(data.loc[periode[kwartaal]['start'],['Start Waarde']][0])
#         stortingen.append((data.loc[periode[kwartaal]['start']:periode[kwartaal]['end'],['Stortingen']]).sum()[0])
#         deponeringen.append((data.loc[periode[kwartaal]['start']:periode[kwartaal]['end'],['Deponeringen']]).sum()[0])
#         onttrekkingen.append((data.loc[periode[kwartaal]['start']:periode[kwartaal]['end'],['Onttrekkingen']]).sum()[0])
#         lichtingen.append((data.loc[periode[kwartaal]['start']:periode[kwartaal]['end'],['Lichtingen']]).sum()[0])
#         eindwaarde.append(data.loc[periode[kwartaal]['end'],['Eind Waarde']][0])




#     overview = list(zip(startwaarde, stortingen, deponeringen, onttrekkingen, lichtingen, eindwaarde))
    
#     df = pd.DataFrame(overview, 
#            columns=["Start Waarde","Stortingen","Deponeringen","Onttrekkingen","Lichtingen","Eind Waarde"], index = kwartaals)


#     df['Abs Rendement'] = df['Eind Waarde'] - df['Start Waarde'] - df['Stortingen'] - df['Deponeringen'] + df['Onttrekkingen'] + df['Lichtingen']

#     df['Rendement'] = (df['Eind Waarde'] - df['Start Waarde']) / df['Start Waarde']

#     return df
                 

# Portefeuille weergave knop en query    
#SELECT "Datum", "Amount_or_Quantity", "Instrument_Name", "Market_Price", "Position_Currency", "Current_Value_in_Position_Currency", "Current_Value_in_EUR" FROM Posrecon WHERE "Account_Number" = VOEG NUMMER IN AND "Datum" = "VOEG DATUM IN " order by "Current_Value_in_EUR"''', con = engine).set_index('Datum')


# Transacties weergave knop en query    
#SELECT "Datum", "Transaction Type Code","Transaction Currency", "Quantity", Instrument_Name", "Price", "Invoice Amount", "Brokerage Fees", "Other Transaction Costs", FROM Traderecon WHERE "Account_Number" = VOEG NUMMER IN  order by "Datum"''', +EVT START EN EINDDATUM con = engine).set_index('Datum')


def users():
    engine = create_engine('sqlite:///DatabaseVB1.db')
    users_df = pd.read_sql('''Select distinct(Account_Number) as Users from Posrecon;''', con = engine) 
    return users_df['Users']