import pandas as pd
import psycopg2 as pg
from statsmodels.tsa.statespace.sarimax import SARIMAX
from datetime import datetime
from pmdarima import auto_arima
from prophet import Prophet

################# DENGUE POR MACROREGIÃO ###########################

conexao2 = pg.connect(database="*****",
                     host="*****",
                     user="*****",
                     password="*****",
                     port="****")

cursor = conexao2.cursor()
consulta_sql = "SELECT to_char(vis.data_notificacao, 'YYYY-MM') AS ano_mes, i.macrorregiao, COUNT(*) AS casos FROM sinan.dengue_bi vis INNER JOIN ibge.ibge_2020 i ON i.cod_ibge_6 = vis.municipio::int GROUP BY ano_mes, i.macrorregiao ORDER BY ano_mes, i.macrorregiao;"
cursor.execute(consulta_sql)
resultados1 = cursor.fetchall()
dengue = pd.DataFrame(resultados1, columns=[desc[0] for desc in cursor.description])


dengue_gfloripa = dengue[dengue['macrorregiao']=='GRANDE FLORIANOPOLIS']
dengue_fozitajai = dengue[dengue['macrorregiao']=='FOZ DO RIO ITAJAI']
dengue_goeste = dengue[dengue['macrorregiao']=='GRANDE OESTE']
dengue_sul = dengue[dengue['macrorregiao']=='SUL']
dengue_meio_oeste_serra = dengue[dengue['macrorregiao']=='MEIO OESTE E SERRA']
dengue_nort_nodest = dengue[dengue['macrorregiao']=='NORTE E NORDESTE']
dengue_valeitajai = dengue[dengue['macrorregiao']=='VALE DO ITAJAI']

dengue.to_csv('dengue.xlsx')
# #### ========================================================================== ##########




# Convertendo a coluna 'ano_mes' para datetime e preparando o DataFrame
dengue_fozitajai['ano_mes'] = pd.to_datetime(dengue_fozitajai['ano_mes'])
df_prophet = dengue_fozitajai.rename(columns={'ano_mes': 'ds', 'casos': 'y'})

# Instanciando e configurando o modelo Prophet
modelo_prophet = Prophet(daily_seasonality=False, yearly_seasonality=True, weekly_seasonality=False,
                         seasonality_mode='multiplicative', seasonality_prior_scale=10)
modelo_prophet.fit(df_prophet)

# Criando datas futuras para previsão
future = modelo_prophet.make_future_dataframe(periods=12, freq='M')

# Previsões
forecast = modelo_prophet.predict(future)

# Extraindo as previsões e preparando o DataFrame final
forecast_filtered = forecast[['ds', 'yhat']].rename(columns={'ds': 'ano_mes', 'yhat': 'casos'})
forecast_filtered['casos'] = forecast_filtered['casos'].abs().astype(int)
forecast_filtered['macrorregiao'] = 'FOZ DO RIO ITAJAI'

# Concatenando os dados reais com as previsões
final_df2 = pd.concat([df_prophet.rename(columns={'ds': 'ano_mes', 'y': 'casos'}), forecast_filtered], ignore_index=True)
final_df2

final_df2 = final_df2.sort_values('ano_mes')

dengue_fozitajai = dengue_fozitajai.sort_values('ano_mes')


forecast_filtered = forecast_filtered.tail(12)

forecast_filtered['numero_mes'] = pd.to_datetime(forecast_filtered['ano_mes'].astype(str)).dt.strftime('%B') #adicionar o nome do mes dentro da coluna
forecast_filtered['ano'] = pd.to_datetime(forecast_filtered['ano_mes'].astype(str)).dt.strftime('%Y') #adicionar o nome do ano dentro da coluna
meses_dict = {  
    'January': '01',
    'February': '02',
    'March': '03',
    'April': '04',
    'May': '05',
    'June': '06',
    'July': '07',
    'August': '08',
    'September': '09',
    'October': '10',
    'November': '11',
    'December': '12'
}
forecast_filtered['numero_mes'] = forecast_filtered['numero_mes'].map(meses_dict)
forecast_filtered['casos'] = forecast_filtered['casos'].abs()
forecast_filtered['previsto'] = 'true'

forecast_filtered['doenca'] = 'Dengue'
forecast_filtered
# #### ========================================================================== ##########



# Convertendo a coluna 'ano_mes' para datetime e preparando o DataFrame
dengue_gfloripa['ano_mes'] = pd.to_datetime(dengue_gfloripa['ano_mes'])
df_prophet2 = dengue_gfloripa.rename(columns={'ano_mes': 'ds', 'casos': 'y'})

# Instanciando e configurando o modelo Prophet
modelo_prophet2 = Prophet(daily_seasonality=False, yearly_seasonality=True, weekly_seasonality=False,
                         seasonality_mode='multiplicative', seasonality_prior_scale=10)
modelo_prophet2.fit(df_prophet2)

# Criando datas futuras para previsão
future2 = modelo_prophet2.make_future_dataframe(periods=12, freq='M')

# Previsões
forecast2 = modelo_prophet2.predict(future2)



# Extraindo as previsões e preparando o DataFrame final
forecast_filtered2 = forecast2[['ds', 'yhat']].rename(columns={'ds': 'ano_mes', 'yhat': 'casos'})
forecast_filtered2['casos'] = forecast_filtered2['casos'].abs().astype(int)
forecast_filtered2['macrorregiao'] = 'GRANDE FLORIANOPOLIS'

# Concatenando os dados reais com as previsões
final_df2 = pd.concat([df_prophet2.rename(columns={'ds': 'ano_mes', 'y': 'casos'}), forecast_filtered2], ignore_index=True)
final_df2

final_df2 = final_df2.sort_values('ano_mes')

dengue_gfloripa = dengue_gfloripa.sort_values('ano_mes')

forecast_filtered2 = forecast_filtered2.tail(12)

forecast_filtered2['numero_mes'] = pd.to_datetime(forecast_filtered2['ano_mes'].astype(str)).dt.strftime('%B') #adicionar o nome do mes dentro da coluna
forecast_filtered2['ano'] = pd.to_datetime(forecast_filtered2['ano_mes'].astype(str)).dt.strftime('%Y') #adicionar o nome do ano dentro da coluna
meses_dict = {  
    'January': '01',
    'February': '02',
    'March': '03',
    'April': '04',
    'May': '05',
    'June': '06',
    'July': '07',
    'August': '08',
    'September': '09',
    'October': '10',
    'November': '11',
    'December': '12'
}
forecast_filtered2['numero_mes'] = forecast_filtered2['numero_mes'].map(meses_dict)
forecast_filtered2['casos'] = forecast_filtered2['casos'].abs()
forecast_filtered2['previsto'] = 'true'

forecast_filtered2['doenca'] = 'Dengue'
# #### ========================================================================== ##########

# Convertendo a coluna 'ano_mes' para datetime e preparando o DataFrame
dengue_goeste['ano_mes'] = pd.to_datetime(dengue_goeste['ano_mes'])
df_prophet3 = dengue_goeste.rename(columns={'ano_mes': 'ds', 'casos': 'y'})

# Instanciando e configurando o modelo Prophet
modelo_prophet3 = Prophet(daily_seasonality=False, yearly_seasonality=True, weekly_seasonality=False,
                         seasonality_mode='multiplicative')
modelo_prophet3.fit(df_prophet3)

# Criando datas futuras para previsão
future3 = modelo_prophet3.make_future_dataframe(periods=12, freq='M')

# Previsões
future3 = modelo_prophet3.predict(future3)

# Extraindo as previsões e preparando o DataFrame final
forecast_filtered3 = future3[['ds', 'yhat']].rename(columns={'ds': 'ano_mes', 'yhat': 'casos'})
forecast_filtered3['casos'] = forecast_filtered3['casos'].abs().astype(int)
forecast_filtered3['macrorregiao'] = 'GRANDE OESTE'

# Concatenando os dados reais com as previsões
final_df3 = pd.concat([df_prophet3.rename(columns={'ds': 'ano_mes', 'y': 'casos'}), forecast_filtered3], ignore_index=True)
final_df3

final_df3 = final_df3.sort_values('ano_mes')

dengue_goeste = dengue_goeste.sort_values('ano_mes')

forecast_filtered3 = forecast_filtered3.tail(12)

forecast_filtered3['numero_mes'] = pd.to_datetime(forecast_filtered3['ano_mes'].astype(str)).dt.strftime('%B') #adicionar o nome do mes dentro da coluna
forecast_filtered3['ano'] = pd.to_datetime(forecast_filtered3['ano_mes'].astype(str)).dt.strftime('%Y') #adicionar o nome do ano dentro da coluna
meses_dict = {  
    'January': '01',
    'February': '02',
    'March': '03',
    'April': '04',
    'May': '05',
    'June': '06',
    'July': '07',
    'August': '08',
    'September': '09',
    'October': '10',
    'November': '11',
    'December': '12'
}
forecast_filtered3['numero_mes'] = forecast_filtered3['numero_mes'].map(meses_dict)
forecast_filtered3['casos'] = forecast_filtered3['casos'].abs()
forecast_filtered3['previsto'] = 'true'

forecast_filtered3['doenca'] = 'Dengue'


# #### ========================================================================== ##########


# Convertendo a coluna 'ano_mes' para datetime e preparando o DataFrame
dengue_sul['ano_mes'] = pd.to_datetime(dengue_sul['ano_mes'])
df_prophet4 = dengue_sul.rename(columns={'ano_mes': 'ds', 'casos': 'y'})

# Instanciando e configurando o modelo Prophet
modelo_prophet4 = Prophet(daily_seasonality=False, yearly_seasonality=True, weekly_seasonality=False,
                         seasonality_mode='multiplicative')
modelo_prophet4.fit(df_prophet4)

# Criando datas futuras para previsão
future4 = modelo_prophet4.make_future_dataframe(periods=12, freq='M')

# Previsões
future4 = modelo_prophet4.predict(future4)

# Extraindo as previsões e preparando o DataFrame final
forecast_filtered4 = future4[['ds', 'yhat']].rename(columns={'ds': 'ano_mes', 'yhat': 'casos'})
forecast_filtered4['casos'] = forecast_filtered4['casos'].abs().astype(int)
forecast_filtered4['macrorregiao'] = 'SUL'

# Concatenando os dados reais com as previsões
final_df4 = pd.concat([df_prophet4.rename(columns={'ds': 'ano_mes', 'y': 'casos'}), forecast_filtered4], ignore_index=True)
final_df4

final_df4 = final_df4.sort_values('ano_mes')

dengue_sul = dengue_sul.sort_values('ano_mes')

forecast_filtered4 = forecast_filtered4.tail(12)

forecast_filtered4['numero_mes'] = pd.to_datetime(forecast_filtered4['ano_mes'].astype(str)).dt.strftime('%B') #adicionar o nome do mes dentro da coluna
forecast_filtered4['ano'] = pd.to_datetime(forecast_filtered4['ano_mes'].astype(str)).dt.strftime('%Y') #adicionar o nome do ano dentro da coluna
meses_dict = {  
    'January': '01',
    'February': '02',
    'March': '03',
    'April': '04',
    'May': '05',
    'June': '06',
    'July': '07',
    'August': '08',
    'September': '09',
    'October': '10',
    'November': '11',
    'December': '12'
}
forecast_filtered4['numero_mes'] = forecast_filtered4['numero_mes'].map(meses_dict)
forecast_filtered4['casos'] = forecast_filtered4['casos'].abs()
forecast_filtered4['previsto'] = 'true'

forecast_filtered4['doenca'] = 'Dengue'
forecast_filtered4
# #### ========================================================================== ##########


# Convertendo a coluna 'ano_mes' para datetime e preparando o DataFrame
dengue_meio_oeste_serra['ano_mes'] = pd.to_datetime(dengue_meio_oeste_serra['ano_mes'])
df_prophet5 = dengue_meio_oeste_serra.rename(columns={'ano_mes': 'ds', 'casos': 'y'})

# Instanciando e configurando o modelo Prophet
modelo_prophet5 = Prophet(daily_seasonality=False, yearly_seasonality=True, weekly_seasonality=False,
                         seasonality_mode='multiplicative')
modelo_prophet5.fit(df_prophet5)

# Criando datas futuras para previsão
future5 = modelo_prophet5.make_future_dataframe(periods=12, freq='M')

# Previsões
future5 = modelo_prophet5.predict(future5)

# Extraindo as previsões e preparando o DataFrame final
forecast_filtered5 = future5[['ds', 'yhat']].rename(columns={'ds': 'ano_mes', 'yhat': 'casos'})
forecast_filtered5['casos'] = forecast_filtered5['casos'].abs().astype(int)
forecast_filtered5['macrorregiao'] = 'MEIO OESTE E SERRA'

# Concatenando os dados reais com as previsões
final_df5 = pd.concat([df_prophet5.rename(columns={'ds': 'ano_mes', 'y': 'casos'}), forecast_filtered5], ignore_index=True)
final_df5

final_df5 = final_df5.sort_values('ano_mes')

dengue_meio_oeste_serra = dengue_meio_oeste_serra.sort_values('ano_mes')

forecast_filtered5 = forecast_filtered5.tail(12)

forecast_filtered5['numero_mes'] = pd.to_datetime(forecast_filtered5['ano_mes'].astype(str)).dt.strftime('%B') #adicionar o nome do mes dentro da coluna
forecast_filtered5['ano'] = pd.to_datetime(forecast_filtered5['ano_mes'].astype(str)).dt.strftime('%Y') #adicionar o nome do ano dentro da coluna
meses_dict = {  
    'January': '01',
    'February': '02',
    'March': '03',
    'April': '04',
    'May': '05',
    'June': '06',
    'July': '07',
    'August': '08',
    'September': '09',
    'October': '10',
    'November': '11',
    'December': '12'
}
forecast_filtered5['numero_mes'] = forecast_filtered5['numero_mes'].map(meses_dict)
forecast_filtered5['casos'] = forecast_filtered5['casos'].abs()
forecast_filtered5['previsto'] = 'true'

forecast_filtered5['doenca'] = 'Dengue'
forecast_filtered5
# #### ========================================================================== ##########


# Convertendo a coluna 'ano_mes' para datetime e preparando o DataFrame
dengue_nort_nodest['ano_mes'] = pd.to_datetime(dengue_nort_nodest['ano_mes'])
df_prophet6 = dengue_nort_nodest.rename(columns={'ano_mes': 'ds', 'casos': 'y'})

# Instanciando e configurando o modelo Prophet
modelo_prophet6 = Prophet(daily_seasonality=False, yearly_seasonality=True, weekly_seasonality=False,
                         seasonality_mode='multiplicative')
modelo_prophet6.fit(df_prophet6)

# Criando datas futuras para previsão
future6 = modelo_prophet6.make_future_dataframe(periods=12, freq='M')

# Previsões
future6 = modelo_prophet6.predict(future6)

# Extraindo as previsões e preparando o DataFrame final
forecast_filtered6 = future6[['ds', 'yhat']].rename(columns={'ds': 'ano_mes', 'yhat': 'casos'})
forecast_filtered6['casos'] = forecast_filtered6['casos'].abs().astype(int)
forecast_filtered6['macrorregiao'] = 'NORTE E NORDESTE'

# Concatenando os dados reais com as previsões
final_df6 = pd.concat([df_prophet6.rename(columns={'ds': 'ano_mes', 'y': 'casos'}), forecast_filtered6], ignore_index=True)


final_df6 = final_df6.sort_values('ano_mes')

dengue_nort_nodest = dengue_nort_nodest.sort_values('ano_mes')

forecast_filtered6 = forecast_filtered6.tail(12)

forecast_filtered6['numero_mes'] = pd.to_datetime(forecast_filtered6['ano_mes'].astype(str)).dt.strftime('%B') #adicionar o nome do mes dentro da coluna
forecast_filtered6['ano'] = pd.to_datetime(forecast_filtered6['ano_mes'].astype(str)).dt.strftime('%Y') #adicionar o nome do ano dentro da coluna
meses_dict = {  
    'January': '01',
    'February': '02',
    'March': '03',
    'April': '04',
    'May': '05',
    'June': '06',
    'July': '07',
    'August': '08',
    'September': '09',
    'October': '10',
    'November': '11',
    'December': '12'
}
forecast_filtered6['numero_mes'] = forecast_filtered6['numero_mes'].map(meses_dict)
forecast_filtered6['casos'] = forecast_filtered6['casos'].abs()
forecast_filtered6['previsto'] = 'true'
forecast_filtered6['doenca'] = 'Dengue'
forecast_filtered6
# #### ========================================================================== ##########


# Convertendo a coluna 'ano_mes' para datetime e preparando o DataFrame
dengue_valeitajai['ano_mes'] = pd.to_datetime(dengue_valeitajai['ano_mes'])
df_prophet7 = dengue_valeitajai.rename(columns={'ano_mes': 'ds', 'casos': 'y'})

# Instanciando e configurando o modelo Prophet
modelo_prophet7 = Prophet(daily_seasonality=False, yearly_seasonality=True, weekly_seasonality=False,
                         seasonality_mode='multiplicative')
modelo_prophet7.fit(df_prophet7)

# Criando datas futuras para previsão
future7 = modelo_prophet7.make_future_dataframe(periods=12, freq='M')

# Previsões
future7 = modelo_prophet7.predict(future7)

# Extraindo as previsões e preparando o DataFrame final
forecast_filtered7 = future7[['ds', 'yhat']].rename(columns={'ds': 'ano_mes', 'yhat': 'casos'})
forecast_filtered7['casos'] = forecast_filtered7['casos'].abs().astype(int)
forecast_filtered7['macrorregiao'] = 'VALE DO ITAJAI'

# Concatenando os dados reais com as previsões
final_df6 = pd.concat([df_prophet7.rename(columns={'ds': 'ano_mes', 'y': 'casos'}), forecast_filtered7], ignore_index=True)


final_df6 = final_df6.sort_values('ano_mes')

dengue_valeitajai = dengue_valeitajai.sort_values('ano_mes')


forecast_filtered7 = forecast_filtered7.tail(12)

forecast_filtered7['numero_mes'] = pd.to_datetime(forecast_filtered7['ano_mes'].astype(str)).dt.strftime('%B') #adicionar o nome do mes dentro da coluna
forecast_filtered7['ano'] = pd.to_datetime(forecast_filtered7['ano_mes'].astype(str)).dt.strftime('%Y') #adicionar o nome do ano dentro da coluna
meses_dict = {  
    'January': '01',
    'February': '02',
    'March': '03',
    'April': '04',
    'May': '05',
    'June': '06',
    'July': '07',
    'August': '08',
    'September': '09',
    'October': '10',
    'November': '11',
    'December': '12'
}
forecast_filtered7['numero_mes'] = forecast_filtered7['numero_mes'].map(meses_dict)
forecast_filtered7['previsto'] = 'true'
forecast_filtered7['casos'] = forecast_filtered7['casos'].abs()
forecast_filtered7['doenca'] = 'Dengue'
forecast_filtered7

# Agrupando os dataframes
agrupado1 = pd.concat([forecast_filtered, forecast_filtered2], ignore_index=True)
agrupado2 = pd.concat([agrupado1, forecast_filtered3], ignore_index=True)
agrupado3 = pd.concat([agrupado2, forecast_filtered4], ignore_index=True)
agrupado4 = pd.concat([agrupado3, forecast_filtered5], ignore_index=True)
agrupado5 = pd.concat([agrupado4, forecast_filtered6], ignore_index=True)
agrupado_final_dengue = pd.concat([agrupado5, forecast_filtered7], ignore_index=True)

# ########### ==================================================================== CARGA NO BANCO DE DADOS =================================================================== ###########




###############                             ##################
###############            SIFIC            ##################
###############                             ##################


conn = pg.connect(
     database="*****",
     user="*****",
     password="*****",
     host="*****",
     port="****"
 )

cursor = conexao2.cursor()

# Consulta SQL
consulta_sql ="""
    SELECT
        TO_CHAR(sb.dat_notific, 'YYYY-MM') AS ano_mes,
        i.macrorregiao,
        COUNT(*) AS casos
    FROM
        sinan.sific_bi sb
    INNER JOIN
        ibge.ibge_2020 i ON i.cod_ibge_6 = sb.municipio::int
    GROUP BY
        ano_mes, i.macrorregiao
    ORDER BY
        ano_mes, i.macrorregiao;
"""

cursor.execute(consulta_sql)
resultados = cursor.fetchall()
sific = pd.DataFrame(resultados, columns=[desc[0] for desc in cursor.description])

cursor.close()
conexao2.close()


sific_gfloripa = sific[sific['macrorregiao']=='GRANDE FLORIANOPOLIS']
sific_fozdeitajai = sific[sific['macrorregiao']=='FOZ DO RIO ITAJAI']
sific_goeste = sific[sific['macrorregiao']=='GRANDE OESTE']
sific_sul = sific[sific['macrorregiao']=='SUL']
sific_meio_oeste_serra = sific[sific['macrorregiao']=='MEIO OESTE E SERRA']
sific_nort_nodest = sific[sific['macrorregiao']=='NORTE E NORDESTE']
sific_valeitajai = sific[sific['macrorregiao']=='VALE DO ITAJAI']

########### ==================================================== GRANDE FLORIANOPOLIS

def project_macroregion8(sific_gfloripa, columns_to_project, year, start_month, num_months):
    unique_macroregions = sific_gfloripa['macrorregiao'].unique()
    projections = []

    for macroregion in unique_macroregions:

        data_subset = sific_gfloripa[sific_gfloripa['macrorregiao'] == macroregion]

        if data_subset.empty or 'ano_mes' not in data_subset.columns:
            continue

        data_subset = data_subset.sort_values(by='ano_mes')

        for col in columns_to_project:
            series = data_subset[data_subset['ano_mes'] <= f"{year:04}-{start_month:02}"][col]
            current_series = series.copy()

            # Initialize next_month with the next month after the current date
            next_month = datetime.now() + pd.DateOffset(months=1)

            for i in range(1, num_months + 1):
                try:
                    order = (2, 1, 2)
                    seasonal_order = (2, 1, 2, 12)
                    model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
                    model_fit = model.fit(disp=False)

                    forecast = model_fit.get_forecast(steps=3).predicted_mean.values[0]
                    forecast_value = round(forecast, 0)

                    # Update the current_series with the forecasted value
                    current_series = current_series.append(pd.Series([forecast_value], index=[next_month]))

                    data_subset = pd.concat([data_subset, pd.DataFrame({'ano_mes': [next_month], 'macrorregiao': [macroregion], col: [forecast_value]})], ignore_index=True)

                    # Update next_month for the next iteration
                    next_month = next_month + pd.DateOffset(months=1)
                except Exception as e:
                    # Handle exceptions if necessary
                    print(f"Error in forecast for {macroregion}, month {next_month}: {e}")

            projections.append(data_subset)

    projections_sific_floripa = pd.concat(projections, ignore_index=True)

    return projections_sific_floripa

# Example usage:
columns_to_project = ['casos']
year_to_project = datetime.now().year
start_month = datetime.now().month
num_months = 12

sific_gfloripa['ano_mes'] = pd.to_datetime(sific_gfloripa['ano_mes'])

projections_sific_floripa = project_macroregion8(sific_gfloripa, columns_to_project, year_to_project, start_month, num_months)


############ ==================================================== VALE DO ITAJAI

def project_macroregion9(sific_fozdeitajai, columns_to_project, year, start_month, num_months):
    unique_macroregions = sific_fozdeitajai['macrorregiao'].unique()
    projections = []

    for macroregion in unique_macroregions:
        data_subset = sific_fozdeitajai[sific_fozdeitajai['macrorregiao'] == macroregion]

        if data_subset.empty or 'ano_mes' not in data_subset.columns:
            continue

        data_subset = data_subset.sort_values(by='ano_mes')

        for col in columns_to_project:
            series = data_subset[data_subset['ano_mes'] <= f"{year:04}-{start_month:02}"][col]
            current_series = series.copy()

            # Initialize next_month with the next month after the current date
            next_month = datetime.now() + pd.DateOffset(months=1)

            for i in range(1, num_months + 1):
                try:
                    order = (2, 1, 2)
                    seasonal_order = (2, 1, 2, 12)
                    model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
                    model_fit = model.fit(disp=False)

                    forecast = model_fit.get_forecast(steps=3).predicted_mean.values[0]
                    forecast_value = round(forecast, 0)

                    # Update the current_series with the forecasted value
                    current_series = current_series.append(pd.Series([forecast_value], index=[next_month]))

                    data_subset = pd.concat([data_subset, pd.DataFrame({'ano_mes': [next_month], 'macrorregiao': [macroregion], col: [forecast_value]})], ignore_index=True)

                    # Update next_month for the next iteration
                    next_month = next_month + pd.DateOffset(months=1)
                except Exception as e:
                    # Handle exceptions if necessary
                    print(f"Error in forecast for {macroregion}, month {next_month}: {e}")

            projections.append(data_subset)

    projections_fozdeitajai = pd.concat(projections, ignore_index=True)

    return projections_fozdeitajai

# Example usage:
columns_to_project = ['casos']
year_to_project = datetime.now().year
start_month = datetime.now().month
num_months = 12

sific_fozdeitajai['ano_mes'] = pd.to_datetime(sific_fozdeitajai['ano_mes'])

projections_fozdeitajai= project_macroregion9(sific_fozdeitajai, columns_to_project, year_to_project, start_month, num_months)

########### ==================================================== VALE DO OESTE

def project_macroregion10(sific_goeste, columns_to_project, year, start_month, num_months):
    unique_macroregions = sific_goeste['macrorregiao'].unique()
    projections = []

    for macroregion in unique_macroregions:
        data_subset = sific_goeste[sific_goeste['macrorregiao'] == macroregion]

        if data_subset.empty or 'ano_mes' not in data_subset.columns:
            continue

        data_subset = data_subset.sort_values(by='ano_mes')

        for col in columns_to_project:
            series = data_subset[data_subset['ano_mes'] <= f"{year:04}-{start_month:02}"][col]
            current_series = series.copy()

            # Initialize next_month with the next month after the current date
            next_month = datetime.now() + pd.DateOffset(months=1)

            for i in range(1, num_months + 1):
                try:
                    order = (2, 1, 2)
                    seasonal_order = (2, 1, 2, 12)
                    model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
                    model_fit = model.fit(disp=False)

                    forecast = model_fit.get_forecast(steps=3).predicted_mean.values[0]
                    forecast_value = round(forecast, 0)

                    # Update the current_series with the forecasted value
                    current_series = current_series.append(pd.Series([forecast_value], index=[next_month]))

                    data_subset = pd.concat([data_subset, pd.DataFrame({'ano_mes': [next_month], 'macrorregiao': [macroregion], col: [forecast_value]})], ignore_index=True)

                    # Update next_month for the next iteration
                    next_month = next_month + pd.DateOffset(months=1)
                except Exception as e:
                    # Handle exceptions if necessary
                    print(f"Error in forecast for {macroregion}, month {next_month}: {e}")

            projections.append(data_subset)

    projections_oeste = pd.concat(projections, ignore_index=True)

    return projections_oeste

# Example usage:
columns_to_project = ['casos']
year_to_project = datetime.now().year
start_month = datetime.now().month
num_months = 12

sific_goeste['ano_mes'] = pd.to_datetime(sific_goeste['ano_mes'])

projections_oeste = project_macroregion10(sific_goeste, columns_to_project, year_to_project, start_month, num_months)

########### ==================================================== VALE DO SUL

def project_macroregion11(sific_sul, columns_to_project, year, start_month, num_months):
    unique_macroregions = sific_sul['macrorregiao'].unique()
    projections = []

    for macroregion in unique_macroregions:
        data_subset = sific_sul[sific_sul['macrorregiao'] == macroregion]

        if data_subset.empty or 'ano_mes' not in data_subset.columns:
            continue

        data_subset = data_subset.sort_values(by='ano_mes')

        for col in columns_to_project:
            series = data_subset[data_subset['ano_mes'] <= f"{year:04}-{start_month:02}"][col]
            current_series = series.copy()

            # Initialize next_month with the next month after the current date
            next_month = datetime.now() + pd.DateOffset(months=1)

            for i in range(1, num_months + 1):
                try:
                    order = (2, 1, 2)
                    seasonal_order = (2, 1, 2, 12)
                    model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
                    model_fit = model.fit(disp=False)

                    forecast = model_fit.get_forecast(steps=3).predicted_mean.values[0]
                    forecast_value = round(forecast, 0)

                    # Update the current_series with the forecasted value
                    current_series = current_series.append(pd.Series([forecast_value], index=[next_month]))

                    data_subset = pd.concat([data_subset, pd.DataFrame({'ano_mes': [next_month], 'macrorregiao': [macroregion], col: [forecast_value]})], ignore_index=True)

                    # Update next_month for the next iteration
                    next_month = next_month + pd.DateOffset(months=1)
                except Exception as e:
                    # Handle exceptions if necessary
                    print(f"Error in forecast for {macroregion}, month {next_month}: {e}")

            projections.append(data_subset)

    projections_sul = pd.concat(projections, ignore_index=True)

    return projections_sul

# Example usage:
columns_to_project = ['casos']
year_to_project = datetime.now().year
start_month = datetime.now().month
num_months = 12

sific_sul['ano_mes'] = pd.to_datetime(sific_sul['ano_mes'])

projections_sul = project_macroregion11(sific_sul, columns_to_project, year_to_project, start_month, num_months)


########### ==================================================== OESTE SERRA

def project_macroregion12(sific_meio_oeste_serra, columns_to_project, year, start_month, num_months):
    unique_macroregions = sific_meio_oeste_serra['macrorregiao'].unique()
    projections = []

    for macroregion in unique_macroregions:
        data_subset = sific_meio_oeste_serra[sific_meio_oeste_serra['macrorregiao'] == macroregion]

        if data_subset.empty or 'ano_mes' not in data_subset.columns:
            continue

        data_subset = data_subset.sort_values(by='ano_mes')

        for col in columns_to_project:
            series = data_subset[data_subset['ano_mes'] <= f"{year:04}-{start_month:02}"][col]
            current_series = series.copy()

            # Initialize next_month with the next month after the current date
            next_month = datetime.now() + pd.DateOffset(months=1)

            for i in range(1, num_months + 1):
                try:
                    order = (2, 1, 2)
                    seasonal_order = (2, 1, 2, 12)
                    model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
                    model_fit = model.fit(disp=False)

                    forecast = model_fit.get_forecast(steps=3).predicted_mean.values[0]
                    forecast_value = round(forecast, 0)

                    # Update the current_series with the forecasted value
                    current_series = current_series.append(pd.Series([forecast_value], index=[next_month]))

                    data_subset = pd.concat([data_subset, pd.DataFrame({'ano_mes': [next_month], 'macrorregiao': [macroregion], col: [forecast_value]})], ignore_index=True)

                    # Update next_month for the next iteration
                    next_month = next_month + pd.DateOffset(months=1)
                except Exception as e:
                    # Handle exceptions if necessary
                    print(f"Error in forecast for {macroregion}, month {next_month}: {e}")

            projections.append(data_subset)

    projections_oeste_serra = pd.concat(projections, ignore_index=True)

    return projections_oeste_serra

# Example usage:
columns_to_project = ['casos']
year_to_project = datetime.now().year
start_month = datetime.now().month
num_months = 12

sific_meio_oeste_serra['ano_mes'] = pd.to_datetime(sific_meio_oeste_serra['ano_mes'])

projections_oeste_serra = project_macroregion12(sific_meio_oeste_serra, columns_to_project, year_to_project, start_month, num_months)

########### ==================================================== OESTE NORTE NORDESTE

def project_macroregion13(sific_nort_nodest, columns_to_project, year, start_month, num_months):
    unique_macroregions = sific_nort_nodest['macrorregiao'].unique()
    projections = []

    for macroregion in unique_macroregions:
        data_subset = sific_nort_nodest[sific_nort_nodest['macrorregiao'] == macroregion]

        if data_subset.empty or 'ano_mes' not in data_subset.columns:
            continue

        data_subset = data_subset.sort_values(by='ano_mes')

        for col in columns_to_project:
            series = data_subset[data_subset['ano_mes'] <= f"{year:04}-{start_month:02}"][col]
            current_series = series.copy()

            # Initialize next_month with the next month after the current date
            next_month = datetime.now() + pd.DateOffset(months=1)

            for i in range(1, num_months + 1):
                try:
                    order = (2, 1, 2)
                    seasonal_order = (2, 1, 2, 12)
                    model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
                    model_fit = model.fit(disp=False)

                    forecast = model_fit.get_forecast(steps=3).predicted_mean.values[0]
                    forecast_value = round(forecast, 0)

                    # Update the current_series with the forecasted value
                    current_series = current_series.append(pd.Series([forecast_value], index=[next_month]))

                    data_subset = pd.concat([data_subset, pd.DataFrame({'ano_mes': [next_month], 'macrorregiao': [macroregion], col: [forecast_value]})], ignore_index=True)

                    # Update next_month for the next iteration
                    next_month = next_month + pd.DateOffset(months=1)
                except Exception as e:
                    # Handle exceptions if necessary
                    print(f"Error in forecast for {macroregion}, month {next_month}: {e}")

            projections.append(data_subset)

    projections_nort_nordest = pd.concat(projections, ignore_index=True)

    return projections_nort_nordest

# Example usage:
columns_to_project = ['casos']
year_to_project = datetime.now().year
start_month = datetime.now().month
num_months = 12

sific_nort_nodest['ano_mes'] = pd.to_datetime(sific_nort_nodest['ano_mes'])

projections_nort_nordest = project_macroregion13(sific_nort_nodest, columns_to_project, year_to_project, start_month, num_months)


########### ==================================================== OESTE NORTE NORDESTE

def project_macroregion14(sific_valeitajai, columns_to_project, year, start_month, num_months):
    unique_macroregions = sific_valeitajai['macrorregiao'].unique()
    projections = []

    for macroregion in unique_macroregions:
        data_subset = sific_valeitajai[sific_valeitajai['macrorregiao'] == macroregion]

        if data_subset.empty or 'ano_mes' not in data_subset.columns:
            continue

        data_subset = data_subset.sort_values(by='ano_mes')

        for col in columns_to_project:
            series = data_subset[data_subset['ano_mes'] <= f"{year:04}-{start_month:02}"][col]
            current_series = series.copy()

            # Initialize next_month with the next month after the current date
            next_month = datetime.now() + pd.DateOffset(months=1)

            for i in range(1, num_months + 1):
                try:
                    order = (2, 1, 2)
                    seasonal_order = (2, 1, 2, 12)
                    model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
                    model_fit = model.fit(disp=False)

                    forecast = model_fit.get_forecast(steps=3).predicted_mean.values[0]
                    forecast_value = round(forecast, 0)

                    # Update the current_series with the forecasted value
                    current_series = current_series.append(pd.Series([forecast_value], index=[next_month]))

                    data_subset = pd.concat([data_subset, pd.DataFrame({'ano_mes': [next_month], 'macrorregiao': [macroregion], col: [forecast_value]})], ignore_index=True)

                    # Update next_month for the next iteration
                    next_month = next_month + pd.DateOffset(months=1)
                except Exception as e:
                    # Handle exceptions if necessary
                    print(f"Error in forecast for {macroregion}, month {next_month}: {e}")

            projections.append(data_subset)

    projections_valeitajai = pd.concat(projections, ignore_index=True)

    return projections_valeitajai

# Example usage:
columns_to_project = ['casos']
year_to_project = datetime.now().year
start_month = datetime.now().month
num_months = 12
sific_valeitajai['ano_mes'] = pd.to_datetime(sific_valeitajai['ano_mes'])
projections_valeitajai = project_macroregion14(sific_valeitajai, columns_to_project, year_to_project, start_month, num_months)

# ### ==========================================================  ====================================================================== ###

projections_sific_floripa = projections_sific_floripa.tail(12)
projections_valeitajai = projections_valeitajai.tail(12)
projections_oeste = projections_oeste.tail(12)
projections_sul = projections_sul.tail(12)
projections_oeste_serra = projections_oeste_serra.tail(12)
projections_nort_nordest = projections_nort_nordest.tail(12)
projections_fozdeitajai = projections_fozdeitajai.tail(12)



# Lista de todos os dataframes que deseja concatenar
projections_list = [projections_sific_floripa, projections_valeitajai, projections_oeste, projections_sul, projections_oeste_serra, projections_nort_nordest, projections_fozdeitajai]

# Concatenar todos os dataframes na lista
resultado_final_sific = pd.concat(projections_list, axis=0)

# ### ========================================================== COLUNAS ====================================================================== ###


resultado_final_sific['numero_mes'] = pd.to_datetime(resultado_final_sific['ano_mes'].astype(str)).dt.strftime('%B') #adicionar o nome do mes dentro da coluna
resultado_final_sific['ano'] = pd.to_datetime(resultado_final_sific['ano_mes'].astype(str)).dt.strftime('%Y') #adicionar o nome do ano dentro da coluna
meses_dict = {  
    'January': '01',
    'February': '02',
    'March': '03',
    'April': '04',
    'May': '05',
    'June': '06',
    'July': '07',
    'August': '08',
    'September': '09',
    'October': '10',
    'November': '11',
    'December': '12'
}
resultado_final_sific['numero_mes'] = resultado_final_sific['numero_mes'].map(meses_dict)

resultado_final_sific['previsto'] = 'true'
resultado_final_sific['casos'] = resultado_final_sific['casos'].abs()
#resultado_df1['previsto'] = resultado_df1['ano_mes'] >= '2023-10'
resultado_final_sific['doenca'] = 'Sific'


# Agrupando os dataframes
agrupado_final = pd.concat([agrupado_final_dengue, resultado_final_sific], ignore_index=True)

# ########### ==================================================================== CARGA NO BANCO DE DADOS =================================================================== ###########

agrupado_final = agrupado_final[agrupado_final['previsto'] == 'true']


conn = pg.connect(
     database="*****",
     user="*****",
     password="*****",
     host="*****",
     port="****"
 )
try:
     cursor = conn.cursor()
     conn.autocommit = False
     cursor.execute('TRUNCATE TABLE projetados.pri_doencas_macrorregional_projetado')
     for index, row in agrupado_final.iterrows():
         cursor.execute(
             'INSERT INTO projetados.pri_doencas_macrorregional_projetado '
             '("ano_mes", "macrorregiao", "casos", "numero_mes", "ano", "previsto", "doenca") '
             'VALUES (%s, %s, %s, %s, %s, %s, %s);',
             (
                 row["ano_mes"],
                 row["macrorregiao"],
                 row["casos"],
                 row["numero_mes"],
                 row["ano"],
                 row["previsto"],
                 row["doenca"]
             )
         )
     conn.commit()
except Exception as e:
     conn.rollback()
     print("Erro:", e)
finally:
     cursor.close()
     conn.close()








# ##################                                     ##################
# ##################            FEBRE AMARELA            ##################
# ##################                                     ##################




# conexao2 = pg.connect(database="*****",
#                      host="*****",
#                      user="*****",
#                      password="*****",
#                      port="****")

# cursor = conexao2.cursor()

# # Consulta SQL
# consulta_sql = """
# SELECT
#     TO_CHAR(fab.dt_notific, 'YYYY-MM') AS ano_mes,
#     i.macrorregiao, -- Adicionado o agrupamento por município
#     COUNT(*) AS casos
# FROM
#     sinan.febre_amarela_bi fab
# INNER JOIN
#     ibge.ibge_2020 i ON i.cod_ibge_6 = fab.id_mn_resi::int
# GROUP BY
#     ano_mes, i.macrorregiao
# ORDER BY
#     ano_mes, i.macrorregiao;
# """

# cursor.execute(consulta_sql)
# resultados = cursor.fetchall()
# febre_amarela_bi = pd.DataFrame(resultados, columns=[desc[0] for desc in cursor.description])

# cursor.close()
# conexao2.close()


# febreamarela_gfloripa = febre_amarela_bi[febre_amarela_bi['macrorregiao']=='GRANDE FLORIANOPOLIS']
# febreamarela_fozrioitajai = febre_amarela_bi[febre_amarela_bi['macrorregiao']=='FOZ DO RIO ITAJAI']
# febreamarela_goeste = febre_amarela_bi[febre_amarela_bi['macrorregiao']=='GRANDE OESTE']
# febreamarela_sul = febre_amarela_bi[febre_amarela_bi['macrorregiao']=='SUL']
# febreamarela_meio_oeste_serra = febre_amarela_bi[febre_amarela_bi['macrorregiao']=='MEIO OESTE E SERRA']
# febreamarela_nort_nodest = febre_amarela_bi[febre_amarela_bi['macrorregiao']=='NORTE E NORDESTE']
# febreamarela_valeitajai = febre_amarela_bi[febre_amarela_bi['macrorregiao']=='VALE DO ITAJAI']


# ############ ==================================================== GRANDE FLORIANOPOLIS

# def project_macroregion15(febreamarela_gfloripa, columns_to_project, year, start_month, num_months):
#     unique_macroregions = febreamarela_gfloripa['macrorregiao'].unique()
#     projections = []

#     for macroregion in unique_macroregions:
#         data_subset = febreamarela_gfloripa[febreamarela_gfloripa['macrorregiao'] == macroregion]

#         if data_subset.empty or 'ano_mes' not in data_subset.columns:
#             continue

#         data_subset = data_subset.sort_values(by='ano_mes')

#         for col in columns_to_project:
#             series = data_subset[data_subset['ano_mes'] <= f"{year:04}-{start_month:02}"][col]
#             current_series = series.copy()

#             # Initialize next_month with the next month after the current date
#             next_month = datetime.now() + pd.DateOffset(months=1)

#             for i in range(1, num_months + 1):
#                 try:
#                     order = (1, 1, 1)
#                     seasonal_order = (1, 1, 1, 12)
#                     model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
#                     model_fit = model.fit(disp=False)

#                     forecast = model_fit.get_forecast(steps=3).predicted_mean.values[0]
#                     forecast_value = round(forecast, 0)

#                     # Update the current_series with the forecasted value
#                     current_series = current_series.append(pd.Series([forecast_value], index=[next_month]))

#                     data_subset = pd.concat([data_subset, pd.DataFrame({'ano_mes': [next_month], 'macrorregiao': [macroregion], col: [forecast_value]})], ignore_index=True)

#                     # Update next_month for the next iteration
#                     next_month = next_month + pd.DateOffset(months=1)
#                 except Exception as e:
#                     # Handle exceptions if necessary
#                     print(f"Error in forecast for {macroregion}, month {next_month}: {e}")

#             projections.append(data_subset)

#     projections_febreamarela_gfloripa = pd.concat(projections, ignore_index=True)

#     return projections_febreamarela_gfloripa

# # Example usage:
# columns_to_project = ['casos']
# year_to_project = datetime.now().year
# start_month = datetime.now().month
# num_months = 12

# febreamarela_gfloripa['ano_mes'] = pd.to_datetime(febreamarela_gfloripa['ano_mes'])

# projections_febreamarela_gfloripa = project_macroregion15(febreamarela_gfloripa, columns_to_project, year_to_project, start_month, num_months)




# # ############ ==================================================== FOZ DO RIO ITAJAI

# def project_macroregion16(febreamarela_fozrioitajai, columns_to_project, year, start_month, num_months):
#     unique_macroregions = febreamarela_fozrioitajai['macrorregiao'].unique()
#     projections = []

#     for macroregion in unique_macroregions:
#         data_subset = febreamarela_fozrioitajai[febreamarela_fozrioitajai['macrorregiao'] == macroregion]

#         if data_subset.empty or 'ano_mes' not in data_subset.columns:
#             continue

#         data_subset = data_subset.sort_values(by='ano_mes')

#         for col in columns_to_project:
#             series = data_subset[data_subset['ano_mes'] <= f"{year:04}-{start_month:02}"][col]
#             current_series = series.copy()

#             # Initialize next_month with the next month after the current date
#             next_month = datetime.now() + pd.DateOffset(months=1)

#             for i in range(1, num_months + 1):
#                 try:
#                     order = (1, 1, 1)
#                     seasonal_order = (1, 1, 1, 12)
#                     model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
#                     model_fit = model.fit(disp=False)

#                     forecast = model_fit.get_forecast(steps=3).predicted_mean.values[0]
#                     forecast_value = round(forecast, 0)

#                     # Update the current_series with the forecasted value
#                     current_series = current_series.append(pd.Series([forecast_value], index=[next_month]))

#                     data_subset = pd.concat([data_subset, pd.DataFrame({'ano_mes': [next_month], 'macrorregiao': [macroregion], col: [forecast_value]})], ignore_index=True)

#                     # Update next_month for the next iteration
#                     next_month = next_month + pd.DateOffset(months=1)
#                 except Exception as e:
#                     # Handle exceptions if necessary
#                     print(f"Error in forecast for {macroregion}, month {next_month}: {e}")

#             projections.append(data_subset)

#     projections_febreamarela_fozrioitajai = pd.concat(projections, ignore_index=True)

#     return projections_febreamarela_fozrioitajai

# # Example usage:
# columns_to_project = ['casos']
# year_to_project = datetime.now().year
# start_month = datetime.now().month
# num_months = 12

# febreamarela_fozrioitajai['ano_mes'] = pd.to_datetime(febreamarela_fozrioitajai['ano_mes'])

# projections_febreamarela_fozrioitajai = project_macroregion16(febreamarela_fozrioitajai, columns_to_project, year_to_project, start_month, num_months)



############ ==================================================== GRANDE OESTE


# def project_macroregion17(febreamarela_goeste, columns_to_project, year, start_month, num_months):
#     unique_macroregions = febreamarela_goeste['macrorregiao'].unique()
#     projections = []

#     for macroregion in unique_macroregions:
#         data_subset = febreamarela_goeste[febreamarela_goeste['macrorregiao'] == macroregion]

#         if data_subset.empty or 'ano_mes' not in data_subset.columns:
#             continue

#         data_subset = data_subset.sort_values(by='ano_mes')

#         for col in columns_to_project:
#             series = data_subset[data_subset['ano_mes'] <= f"{year:04}-{start_month:02}"][col]
#             current_series = series.copy()

#             # Initialize next_month with the next month after the current date
#             next_month = datetime.now() + pd.DateOffset(months=1)

#             for i in range(1, num_months + 1):
#                 try:
#                     order = (1, 1, 1)
#                     seasonal_order = (1, 1, 1, 12)
#                     model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
#                     model_fit = model.fit(disp=False)

#                     forecast = model_fit.get_forecast(steps=3).predicted_mean.values[0]
#                     forecast_value = round(forecast, 0)

#                     # Update the current_series with the forecasted value
#                     current_series = current_series.append(pd.Series([forecast_value], index=[next_month]))

#                     data_subset = pd.concat([data_subset, pd.DataFrame({'ano_mes': [next_month], 'macrorregiao': [macroregion], col: [forecast_value]})], ignore_index=True)

#                     # Update next_month for the next iteration
#                     next_month = next_month + pd.DateOffset(months=1)
#                 except Exception as e:
#                     # Handle exceptions if necessary
#                     print(f"Error in forecast for {macroregion}, month {next_month}: {e}")

#             projections.append(data_subset)

#     projections_febreamarela_fozrioitajai = pd.concat(projections, ignore_index=True)

#     return projections_febreamarela_fozrioitajai

# # Example usage:
# columns_to_project = ['casos']
# year_to_project = datetime.now().year
# start_month = datetime.now().month
# num_months = 12

# febreamarela_goeste['ano_mes'] = pd.to_datetime(febreamarela_goeste['ano_mes'])

# projections_febreamarela_goeste = project_macroregion17(febreamarela_goeste, columns_to_project, year_to_project, start_month, num_months)





############ ==================================================== SUL

# def project_macroregion18(febreamarela_sul, columns_to_project, year, start_month, num_months):
#     unique_macroregions = febreamarela_sul['macrorregiao'].unique()
#     projections = []

#     for macroregion in unique_macroregions:
#         data_subset = febreamarela_sul[febreamarela_sul['macrorregiao'] == macroregion]

#         if data_subset.empty or 'ano_mes' not in data_subset.columns:
#             continue

#         data_subset = data_subset.sort_values(by='ano_mes')

#         for col in columns_to_project:
#             series = data_subset[data_subset['ano_mes'] <= f"{year:04}-{start_month:02}"][col]
#             current_series = series.copy()

#             # Initialize next_month with the next month after the current date
#             next_month = datetime.now() + pd.DateOffset(months=1)

#             for i in range(1, num_months + 1):
#                 try:
#                     order = (1, 1, 1)
#                     seasonal_order = (1, 1, 1, 12)
#                     model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
#                     model_fit = model.fit(disp=False)

#                     forecast = model_fit.get_forecast(steps=3).predicted_mean.values[0]
#                     forecast_value = round(forecast, 0)

#                     # Update the current_series with the forecasted value
#                     current_series = current_series.append(pd.Series([forecast_value], index=[next_month]))

#                     data_subset = pd.concat([data_subset, pd.DataFrame({'ano_mes': [next_month], 'macrorregiao': [macroregion], col: [forecast_value]})], ignore_index=True)

#                     # Update next_month for the next iteration
#                     next_month = next_month + pd.DateOffset(months=1)
#                 except Exception as e:
#                     # Handle exceptions if necessary
#                     print(f"Error in forecast for {macroregion}, month {next_month}: {e}")

#             projections.append(data_subset)

#     projections_febreamarela_sul = pd.concat(projections, ignore_index=True)

#     return projections_febreamarela_sul

# columns_to_project = ['casos']
# year_to_project = datetime.now().year
# start_month = datetime.now().month
# num_months = 12

# febreamarela_sul['ano_mes'] = pd.to_datetime(febreamarela_sul['ano_mes'])

# projections_febreamarela_sul = project_macroregion18(febreamarela_sul, columns_to_project, year_to_project, start_month, num_months)



# ############ ==================================================== OESTE SERRA

# def project_macroregion19(febreamarela_meio_oeste_serra, columns_to_project, year, start_month, num_months):
#     unique_macroregions = febreamarela_meio_oeste_serra['macrorregiao'].unique()
#     projections = []
#     for macroregion in unique_macroregions:
#         data_subset = febreamarela_meio_oeste_serra[febreamarela_meio_oeste_serra['macrorregiao'] == macroregion]
#         if data_subset.empty or 'ano_mes' not in data_subset.columns:
#             continue
#         data_subset = data_subset.sort_values(by='ano_mes')
#         for col in columns_to_project:
#             series = data_subset[data_subset['ano_mes'] <= f"{year:04}-{start_month:02}"][col]
#             current_series = series.copy()
#             next_month = datetime.now() + pd.DateOffset(months=1)
#             for i in range(1, num_months + 1):
#                 try:
#                     order = (1, 1, 1)
#                     seasonal_order = (1, 1, 1, 12)
#                     model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
#                     model_fit = model.fit(disp=False)

#                     forecast = model_fit.get_forecast(steps=3).predicted_mean.values[0]
#                     forecast_value = round(forecast, 0)

#                     current_series = current_series.append(pd.Series([forecast_value], index=[next_month]))

#                     data_subset = pd.concat([data_subset, pd.DataFrame({'ano_mes': [next_month], 'macrorregiao': [macroregion], col: [forecast_value]})], ignore_index=True)

#                     next_month = next_month + pd.DateOffset(months=1)
#                 except Exception as e:
#                     print(f"Error in forecast for {macroregion}, month {next_month}: {e}")

#             projections.append(data_subset)

#     projections_febreamarela_meio_oeste_serra = pd.concat(projections, ignore_index=True)

#     return projections_febreamarela_meio_oeste_serra

# columns_to_project = ['casos']
# year_to_project = datetime.now().year
# start_month = datetime.now().month
# num_months = 12

# febreamarela_meio_oeste_serra['ano_mes'] = pd.to_datetime(febreamarela_meio_oeste_serra['ano_mes'])

# projections_febreamarela_meio_oeste_serra = project_macroregion19(febreamarela_meio_oeste_serra, columns_to_project, year_to_project, start_month, num_months)





# ############ ==================================================== NORTE NORDESTE

# def project_macroregion20(febreamarela_nort_nodest, columns_to_project, year, start_month, num_months):
#     unique_macroregions = febreamarela_nort_nodest['macrorregiao'].unique()
#     projections = []
#     for macroregion in unique_macroregions:
#         data_subset = febreamarela_nort_nodest[febreamarela_nort_nodest['macrorregiao'] == macroregion]
#         if data_subset.empty or 'ano_mes' not in data_subset.columns:
#             continue
#         data_subset = data_subset.sort_values(by='ano_mes')
#         for col in columns_to_project:
#             series = data_subset[data_subset['ano_mes'] <= f"{year:04}-{start_month:02}"][col]
#             current_series = series.copy()
#             next_month = datetime.now() + pd.DateOffset(months=1)
#             for i in range(1, num_months + 1):
#                 try:
#                     order = (1, 1, 1)
#                     seasonal_order = (1, 1, 1, 12)
#                     model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
#                     model_fit = model.fit(disp=False)

#                     forecast = model_fit.get_forecast(steps=3).predicted_mean.values[0]
#                     forecast_value = round(forecast, 0)

#                     current_series = current_series.append(pd.Series([forecast_value], index=[next_month]))

#                     data_subset = pd.concat([data_subset, pd.DataFrame({'ano_mes': [next_month], 'macrorregiao': [macroregion], col: [forecast_value]})], ignore_index=True)

#                     next_month = next_month + pd.DateOffset(months=1)
#                 except Exception as e:
#                     print(f"Error in forecast for {macroregion}, month {next_month}: {e}")

#             projections.append(data_subset)

#     projections_febreamarela_nort_nodest = pd.concat(projections, ignore_index=True)

#     return projections_febreamarela_nort_nodest

# columns_to_project = ['casos']
# year_to_project = datetime.now().year
# start_month = datetime.now().month
# num_months = 12

# febreamarela_nort_nodest['ano_mes'] = pd.to_datetime(febreamarela_nort_nodest['ano_mes'])

# projections_febreamarela_nort_nodest = project_macroregion20(febreamarela_nort_nodest, columns_to_project, year_to_project, start_month, num_months)



# ############ ==================================================== VALE DO ITAJAI

# def project_macroregion21(febreamarela_valeitajai, columns_to_project, year, start_month, num_months):
#     unique_macroregions = febreamarela_valeitajai['macrorregiao'].unique()
#     projections = []
#     for macroregion in unique_macroregions:
#         data_subset = febreamarela_valeitajai[febreamarela_valeitajai['macrorregiao'] == macroregion]
#         if data_subset.empty or 'ano_mes' not in data_subset.columns:
#             continue
#         data_subset = data_subset.sort_values(by='ano_mes')
#         for col in columns_to_project:
#             series = data_subset[data_subset['ano_mes'] <= f"{year:04}-{start_month:02}"][col]
#             current_series = series.copy()
#             next_month = datetime.now() + pd.DateOffset(months=1)
#             for i in range(1, num_months + 1):
#                 try:
#                     order = (1, 1, 1)
#                     seasonal_order = (1, 1, 1, 12)
#                     model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
#                     model_fit = model.fit(disp=False)

#                     forecast = model_fit.get_forecast(steps=3).predicted_mean.values[0]
#                     forecast_value = round(forecast, 0)

#                     current_series = current_series.append(pd.Series([forecast_value], index=[next_month]))

#                     data_subset = pd.concat([data_subset, pd.DataFrame({'ano_mes': [next_month], 'macrorregiao': [macroregion], col: [forecast_value]})], ignore_index=True)

#                     next_month = next_month + pd.DateOffset(months=1)
#                 except Exception as e:
#                     print(f"Error in forecast for {macroregion}, month {next_month}: {e}")

#             projections.append(data_subset)

#     projections_febreamarela_valeitajai = pd.concat(projections, ignore_index=True)

#     return projections_febreamarela_valeitajai
# columns_to_project = ['casos']
# year_to_project = datetime.now().year
# start_month = datetime.now().month
# num_months = 12
# febreamarela_valeitajai['ano_mes'] = pd.to_datetime(febreamarela_valeitajai['ano_mes'])

# projections_febreamarela_valeitajai = project_macroregion21(febreamarela_valeitajai, columns_to_project, year_to_project, start_month, num_months)



# # Lista de todos os dataframes que deseja concatenar
# projections_list = [
#     projections_febreamarela_gfloripa,
#     projections_febreamarela_fozrioitajai,
#     projections_febreamarela_valeitajai,
#     projections_febreamarela_sul,
#     projections_febreamarela_meio_oeste_serra,
#     projections_febreamarela_nort_nodest
# ]

# # Concatenar todos os dataframes na lista
# resultado_final_famarela = pd.concat(projections_list, axis=0)


# ################################### =======================================COLUNAS==================================================== ###################################
# resultado_final_famarela['numero_mes'] = pd.to_datetime(resultado_final_famarela['ano_mes'].astype(str)).dt.strftime('%B') #adicionar o nome do mes dentro da coluna
# resultado_final_famarela['ano'] = pd.to_datetime(resultado_final_famarela['ano_mes'].astype(str)).dt.strftime('%Y') #adicionar o nome do ano dentro da coluna
# meses_dict = {  
#     'January': '01',
#     'February': '02',
#     'March': '03',
#     'April': '04',
#     'May': '05',
#     'June': '06',
#     'July': '07',
#     'August': '08',
#     'September': '09',
#     'October': '10',
#     'November': '11',
#     'December': '12'
# }
# resultado_final_famarela['numero_mes'] = resultado_final_famarela['numero_mes'].map(meses_dict)
# data_atual = datetime.now()
# mes_atual = data_atual.month
# ano_atual = data_atual.year
# resultado_final_famarela['previsto'] = ~(resultado_final_famarela['ano_mes'].apply(lambda x: (x.year, x.month) <= (ano_atual, mes_atual)))
# resultado_final_famarela['casos'] = resultado_final_famarela['casos'].abs()

# #resultado_df1['previsto'] = resultado_df1['ano_mes'] >= '2023-10'
# resultado_final_famarela['doenca'] = 'Febre Amarela'
# ################################### ======================================= MERGE FINAL ==================================================== ###################################


