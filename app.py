import streamlit as st
import pandas as pd
import altair as alt
import time
import datetime
import yfinance as yf
import investpy
from Balans import *

st.sidebar.markdown("# Dashboard")
# Functies voor het inladen van bestanden
if st.sidebar.button('Lees Input Bestanden'):
	LoadData()
# Functie voor het presenteren van de account numbers
userslist = Users()
# Account number aan een variabele toevoegen
reknr = st.sidebar.selectbox('Selecteer een gebruiker', userslist)
# Klanten portefeuille aan een variabele koppelen die verder als input wordt gebruikt 
df = GetRendement(reknr)
# Input velden voor sidebar
st.sidebar.markdown("# Periode")
start_date = st.sidebar.date_input('Start Datum')
end_date = st.sidebar.date_input('Eind Datum')
periode_keuze = st.sidebar.multiselect("Selecteer de gewenste periode voor de portefeuille ontwikkeling", ['Q1','Q2','Q3','Q4'])
st.sidebar.markdown("# Benchmark")

# Voeg hier de tickers in van aandelen die je wilt gebruiken voor de benchmark
bench_stocks = ['^AEX', 'SPYY.DE', 'IUSQ.DE', 'RDSA.AS', 'TSLA', '^DJI', 'GC=F']
benchmark_keuze = st.sidebar.selectbox('Selecteer de Benchmark', bench_stocks)

if st.sidebar.button('Toon Data'):
    if not periode_keuze:
        engine = create_engine('sqlite:///DatabaseVB.db')
        df = GetRendement(reknr)

# Kies hier de start en eind datum
        start_d = start_date.strftime("%Y-%m-%d")
        end_d = end_date.strftime("%Y-%m-%d")
        
# Het systeem kijkt in de database wat de start en eind datums zijn voor de rekeningnummer en geeft deze als output
        database_start_date = pd.read_sql(f'''
            select distinct(datum) from Posrecon where Account_Number = "{reknr}"
            union
            select distinct(datum) from Traderecon where Account_Number = "{reknr}"
            order by datum asc limit 1;
            ''', con = engine)

        database_end_date = pd.read_sql(f'''
            select distinct(datum) from Posrecon where Account_Number = "{reknr}"
            union
            select distinct(datum) from Traderecon where Account_Number = "{reknr}"
            order by datum desc limit 1;
            ''', con = engine)
        
        new_start_date = pd.to_datetime(database_start_date['Datum'][0]).strftime("%Y-%m-%d")
        new_end_date = pd.to_datetime(database_end_date['Datum'][0]).strftime("%Y-%m-%d")


# Hier vergelijken we gekozen start/eind datum en de start/eind datum in de database. Als de gekozen start/eind datum kleiner/groter is dan wat er in de database staat zal deze de nieuwe
# start/eind datum worden
        if start_d < new_start_date:
            start_d = new_start_date
        if end_d > new_end_date:
            end_d = new_end_date

# GUI voor Portefeuille ontwikkeling
        st.markdown("## Portefeuille Ontwikkeling")
        st.markdown(f"#### From {start_d} to {end_d}")

# Table output voor de Portefeuille Ontwikkeling op basis van de Start- en Einddatum als zoek criteria
        st.table(ZoekPortfOntwikkeling(df, start_d, end_d))

# Benchmark Ontwikkeling
        st.markdown(f"## Benchmark Ontwikkeling {benchmark_keuze}")

# Table output van Portefeuille ontwikkeling op basis van Benchmark die wordt gemerged met de Datums van de klant waar de 
# Start waarde, Eind waarde, Absolute rendement en Percentage Rendement worden opgehaald en berekend
        st.table(PortfBenchOverzicht(KlantData(df, getBenchmarkData(benchmark_keuze)), start_d, end_d))

# Grafiek van de Klant Portefeuille Data en bencmark
        ZoekGraph(df, getBenchmarkData(benchmark_keuze), start_d, end_d)

# Portefeuille transacties etc
        st.markdown("### Portefeuille overzicht op eind datum")
        st.dataframe(ShowPortfolio(reknr, end_d))
        st.markdown("### Transactie overzicht gedurende periode")
        st.dataframe(ShowTransaction(reknr))
        st.dataframe(df)
    else:
        st.markdown("## Portefeuille Ontwikkeling")
        df = GetRendement(reknr)
        df_port_ont = st.table(GetOverview(df, periode_keuze))
        st.markdown(f"## Benchmark Ontwikkeling {benchmark_keuze}")
        full_bench_df = getBenchmarkData(benchmark_keuze)
        st.table(getPerf(full_bench_df, periode_keuze, benchmark_keuze))
        Graph(df, getBenchmarkData(benchmark_keuze), benchmark_keuze, periode_keuze)