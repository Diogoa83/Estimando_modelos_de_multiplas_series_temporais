import pandas as pd

import psycopg2
from datetime import datetime
from statsmodels.tsa.statespace.sarimax import SARIMAX
import datetime  
from datetime import datetime 

########################## MORTALIDADE INFANTIL POR MUNICIPIO ##########################

conexao = psycopg2.connect(
    database="******",
    user="******",
    password="******",
    host="******",
    port="****"
)

cursor = conexao.cursor()

consulta = """
SELECT
    TO_CHAR(x.dt_obito, 'YYYY-MM') AS ano_mes,
    x.cd_municipio_residencia as municipio,
    COUNT(*) AS numero_obitos
FROM sim.sim_bi x
WHERE x.de_idade IN ('Dias', 'Horas', 'Minutos', 'Mês') and nu_idade_unidade <= '1'
GROUP BY TO_CHAR(x.dt_obito, 'YYYY-MM'), x.cd_municipio_residencia
ORDER BY TO_CHAR(x.dt_obito, 'YYYY-MM'), x.cd_municipio_residencia;
"""

cursor.execute(consulta)

resultados = cursor.fetchall()

data = pd.DataFrame(resultados, columns=[desc[0] for desc in cursor.description])

data['ano_mes'] = pd.to_datetime(data['ano_mes'], format='%Y-%m')
print(data.info())
print(data)

def project_macroregion(data, columns_to_project, start_month, num_months):
    unique_macroregions = data['municipio'].unique()
    projections = []
    for macroregion in unique_macroregions:
        data_subset = data[data['municipio'] == macroregion]
        if data_subset.empty or 'ano_mes' not in data_subset.columns:
            continue
        data_subset = data_subset.sort_values(by='ano_mes')
        for col in columns_to_project:
            series = data_subset[col]
            current_series = series.copy()  
            for i in range(num_months):
                try:
                    current_date = datetime.now()
                    current_year = current_date.year
                    current_month = start_month + i
                    if current_month > 12:
                        current_month -= 12
                        current_year += 1
                    next_date = datetime(current_year, current_month, 1)
                    order = (1, 1, 1) # Ajuste do modelo SARIMA usando a série atual
                    seasonal_order = (1, 1, 1, 12)
                    model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
                    model_fit = model.fit(disp=False)
                    forecast_value = model_fit.get_forecast(steps=1).predicted_mean.values[0]
                    forecast_value = round(forecast_value, 0)
                    data_subset = data_subset.append({'ano_mes': next_date, 'municipio': macroregion, col: forecast_value}, ignore_index=True)
                    current_series = current_series.append(pd.Series([forecast_value], index=[next_date])) # Atualiza a série atual com o novo valor previsto
                except Exception as e:
                    print(f"Erro ao ajustar o modelo para {macroregion}, mês {current_month}: {str(e)}")
            projections.append(data_subset)
    projections_infantil = pd.concat(projections, ignore_index=True)
    return projections_infantil

columns_to_project = ['numero_obitos']

current_date = datetime.now()
start_month = current_date.month
num_months = 4

data['ano_mes'] = pd.to_datetime(data['ano_mes'])

projections_infantil = project_macroregion(data, columns_to_project, start_month, num_months)

# ########################## CANCER DE COLO DO UTERO ##########################

conexao1 = psycopg2.connect(
    database="******",
    user="******",
    password="******",
    host="******",
    port="****"
)

cursor1 = conexao1.cursor()

consulta1 = """
SELECT
    TO_CHAR(x.dt_obito, 'YYYY-MM') AS ano_mes,
    x.cd_municipio_residencia as municipio,
    COUNT(*) AS numero_obitos
FROM sim.sim_bi x
WHERE x.cd_cid_causa_basica IN ('C530', 'C531', 'C532', 'C533', 'C534', 'C535', 'C536','C537', 'C538', 'C539')
GROUP BY TO_CHAR(x.dt_obito, 'YYYY-MM'), x.cd_municipio_residencia
ORDER BY TO_CHAR(x.dt_obito, 'YYYY-MM'), x.cd_municipio_residencia;
"""

cursor1.execute(consulta1)

resultados1 = cursor1.fetchall()

data5 = pd.DataFrame(resultados1, columns=[desc[0] for desc in cursor1.description])

print(data5)

def project_macroregion(data5, columns_to_project1, start_month, num_months):
    unique_macroregions = data5['municipio'].unique()
    projections = []

    for macroregion in unique_macroregions:
        data_subset = data5[data5['municipio'] == macroregion]

        if data_subset.empty or 'ano_mes' not in data_subset.columns:
            continue

        data_subset = data_subset.sort_values(by='ano_mes')

        for col in columns_to_project1:
            series = data_subset[col]
            current_series = series.copy()  

            for i in range(num_months):
                try:
                    current_date = datetime.now()
                    current_year = current_date.year
                    current_month = start_month + i
                    if current_month > 12:
                        current_month -= 12
                        current_year += 1

                    next_date = datetime(current_year, current_month, 1)

                    # Ajuste do modelo SARIMA usando a série atual
                    order = (1, 1, 1)
                    seasonal_order = (1, 1, 1, 12)
                    model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
                    model_fit = model.fit(disp=False)

                    forecast_value = model_fit.get_forecast(steps=1).predicted_mean.values[0]
                    forecast_value = round(forecast_value, 0)

                    data_subset = data_subset.append({'ano_mes': next_date, 'municipio': macroregion, col: forecast_value}, ignore_index=True)

                    # Atualiza a série atual com o novo valor previsto
                    current_series = current_series.append(pd.Series([forecast_value], index=[next_date]))

                except Exception as e:
                    print(f"Erro ao ajustar o modelo para {macroregion}, mês {current_month}: {str(e)}")

            projections.append(data_subset)

    projections_cancer_utero= pd.concat(projections, ignore_index=True)

    return projections_cancer_utero

columns_to_project1 = ['numero_obitos']

current_date = datetime.now()
start_month = current_date.month
num_months = 4

data5['ano_mes'] = pd.to_datetime(data5['ano_mes'])

projections_cancer_utero = project_macroregion(data5, columns_to_project1, start_month, num_months)

########################## MORTALIDADE INVESTIGADO POR MUNICIPIO ##########################

conexao2 = psycopg2.connect(
    database="******",
    user="******",
    password="******",
    host="******",
    port="****"
)

cursor2 = conexao2.cursor()

consulta = """
SELECT
    TO_CHAR(x.dt_obito, 'YYYY-MM') AS ano_mes,
    x.cd_municipio_residencia as municipio,
    COUNT(*) AS numero_obitos
FROM sim.sim_bi x
WHERE x.de_obito_investigado IN ('Sim') 
GROUP BY TO_CHAR(x.dt_obito, 'YYYY-MM'), x.cd_municipio_residencia
ORDER BY TO_CHAR(x.dt_obito, 'YYYY-MM'), x.cd_municipio_residencia;
"""

cursor2.execute(consulta)

resultados2 = cursor2.fetchall()

data2 = pd.DataFrame(resultados2, columns=[desc[0] for desc in cursor2.description])
print(data2)
def project_macroregion(data2, columns_to_project2, start_month, num_months):
    unique_macroregions = data2['municipio'].unique()
    projections = []

    for macroregion in unique_macroregions:
        data_subset = data2[data2['municipio'] == macroregion]

        if data_subset.empty or 'ano_mes' not in data_subset.columns:
            continue

        data_subset = data_subset.sort_values(by='ano_mes')

        for col in columns_to_project2:
            series = data_subset[col]
            current_series = series.copy()  

            for i in range(num_months):
                try:
                    current_date = datetime.now()
                    current_year = current_date.year
                    current_month = start_month + i
                    if current_month > 12:
                        current_month -= 12
                        current_year += 1

                    next_date = datetime(current_year, current_month, 1)

                    # Ajuste do modelo SARIMA usando a série atual
                    order = (1, 1, 1)
                    seasonal_order = (1, 1, 1, 12)
                    model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
                    model_fit = model.fit(disp=False)

                    forecast_value = model_fit.get_forecast(steps=1).predicted_mean.values[0]
                    forecast_value = round(forecast_value, 0)

                    data_subset = data_subset.append({'ano_mes': next_date, 'municipio': macroregion, col: forecast_value}, ignore_index=True)

                    # Atualiza a série atual com o novo valor previsto
                    current_series = current_series.append(pd.Series([forecast_value], index=[next_date]))

                except Exception as e:
                    print(f"Erro ao ajustar o modelo para {macroregion}, mês {current_month}: {str(e)}")

            projections.append(data_subset)

    projections_investigados = pd.concat(projections, ignore_index=True)

    return projections_investigados

columns_to_project2 = ['numero_obitos']

current_date = datetime.now()
start_month = current_date.month
num_months = 4

data2['ano_mes'] = pd.to_datetime(data2['ano_mes'])

projections_investigados = project_macroregion(data2, columns_to_project2, start_month, num_months)



###### ====================================     ÓBITOS CÂNCER DE MAMA          ================================================================== ####


conexao1 = psycopg2.connect(
    database="******",
    user="******",
    password="******",
    host="******",
    port="****"
)

cursor1 = conexao1.cursor()

consulta1 = """
SELECT
    TO_CHAR(x.dt_obito, 'YYYY-MM') AS ano_mes,
    x.cd_municipio_residencia as municipio,
    COUNT(*) AS numero_obitos
FROM sim.sim_bi x
WHERE x.cd_cid_causa_basica IN ('C500', 'C501', 'C502', 'C503', 'C504', 'C505', 'C506', 'C507', 'C508', 'C509')
GROUP BY TO_CHAR(x.dt_obito, 'YYYY-MM'), x.cd_municipio_residencia
ORDER BY TO_CHAR(x.dt_obito, 'YYYY-MM'), x.cd_municipio_residencia;
"""

cursor1.execute(consulta1)

resultados1 = cursor1.fetchall()

data6 = pd.DataFrame(resultados1, columns=[desc[0] for desc in cursor1.description])

print(data6)

def project_macroregion(data6, columns_to_project1, start_month, num_months):
    unique_macroregions = data6['municipio'].unique()
    projections = []

    for macroregion in unique_macroregions:
        data_subset = data6[data6['municipio'] == macroregion]

        if data_subset.empty or 'ano_mes' not in data_subset.columns:
            continue

        data_subset = data_subset.sort_values(by='ano_mes')

        for col in columns_to_project1:
            series = data_subset[col]
            current_series = series.copy()  

            for i in range(num_months):
                try:
                    current_date = datetime.now()
                    current_year = current_date.year
                    current_month = start_month + i
                    if current_month > 12:
                        current_month -= 12
                        current_year += 1

                    next_date = datetime(current_year, current_month, 1)

                    # Ajuste do modelo SARIMA usando a série atual
                    order = (1, 1, 1)
                    seasonal_order = (1, 1, 1, 12)
                    model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
                    model_fit = model.fit(disp=False)

                    forecast_value = model_fit.get_forecast(steps=1).predicted_mean.values[0]
                    forecast_value = round(forecast_value, 0)

                    data_subset = data_subset.append({'ano_mes': next_date, 'municipio': macroregion, col: forecast_value}, ignore_index=True)

                    # Atualiza a série atual com o novo valor previsto
                    current_series = current_series.append(pd.Series([forecast_value], index=[next_date]))

                except Exception as e:
                    print(f"Erro ao ajustar o modelo para {macroregion}, mês {current_month}: {str(e)}")

            projections.append(data_subset)

    projections_cancer_mama = pd.concat(projections, ignore_index=True)

    return projections_cancer_mama

columns_to_project1 = ['numero_obitos']

current_date = datetime.now()
start_month = current_date.month
num_months = 4

data6['ano_mes'] = pd.to_datetime(data6['ano_mes'])

projections_cancer_mama = project_macroregion(data6, columns_to_project1, start_month, num_months)



################ ==================================  CANCER PROSTATA



conexao1 = psycopg2.connect(
    database="******",
    user="******",
    password="******",
    host="******",
    port="****"
)

cursor1 = conexao1.cursor()

consulta1 = """
SELECT
    TO_CHAR(x.dt_obito, 'YYYY-MM') AS ano_mes,
    x.cd_municipio_residencia AS municipio,
    COUNT(*) AS numero_obitos
FROM 
    sim.sim_bi x
WHERE 
    x.cd_cid_causa_basica LIKE '%C61%'
GROUP BY 
    TO_CHAR(x.dt_obito, 'YYYY-MM'), x.cd_municipio_residencia
ORDER BY 
    TO_CHAR(x.dt_obito, 'YYYY-MM'), x.cd_municipio_residencia;
"""

cursor1.execute(consulta1)

resultados1 = cursor1.fetchall()

data7 = pd.DataFrame(resultados1, columns=[desc[0] for desc in cursor1.description])

print(data7)

def project_macroregion(data7, columns_to_project1, start_month, num_months):
    unique_macroregions = data7['municipio'].unique()
    projections = []

    for macroregion in unique_macroregions:
        data_subset = data7[data7['municipio'] == macroregion]

        if data_subset.empty or 'ano_mes' not in data_subset.columns:
            # Include 'ano_mes' column if empty
            data_subset = pd.DataFrame(columns=['ano_mes', 'municipio'] + columns_to_project1)

        data_subset = data_subset.sort_values(by='ano_mes')

        for col in columns_to_project1:
            series = data_subset[col]
            current_series = series.copy()  

            for i in range(num_months):
                try:
                    current_date = datetime.now()
                    current_year = current_date.year
                    current_month = start_month + i
                    if current_month > 12:
                        current_month -= 12
                        current_year += 1

                    next_date = datetime(current_year, current_month, 1)

                    # Ajuste do modelo SARIMA usando a série atual
                    order = (1, 1, 1)
                    seasonal_order = (1, 1, 1, 12)
                    model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
                    model_fit = model.fit(disp=False)

                    forecast_value = model_fit.get_forecast(steps=1).predicted_mean.values[0]
                    forecast_value = round(forecast_value, 0)

                    data_subset = data_subset.append({'ano_mes': next_date, 'municipio': macroregion, col: forecast_value}, ignore_index=True)

                    current_series = current_series.append(pd.Series([forecast_value], index=[next_date]))

                except Exception as e:
                    print(f"Erro ao ajustar o modelo para {macroregion}, mês {current_month}: {str(e)}")

            projections.append(data_subset)

    if projections:
        projections_cancer_prostata = pd.concat(projections, ignore_index=True)
    else:
        projections_cancer_prostata = pd.DataFrame(columns=['ano_mes', 'municipio'] + columns_to_project1)

    return projections_cancer_prostata

columns_to_project1 = ['numero_obitos']

current_date = datetime.now()
start_month = current_date.month
num_months = 4

data7['ano_mes'] = pd.to_datetime(data7['ano_mes'])

projections_cancer_prostata = project_macroregion(data7, columns_to_project1, start_month, num_months)

########################### ==========================CANCER DE PULMAO ========================================

conexao1 = psycopg2.connect(
    database="******",
    user="******",
    password="******",
    host="******",
    port="****"
)

cursor1 = conexao1.cursor()

consulta1 = """
SELECT
    TO_CHAR(x.dt_obito, 'YYYY-MM') AS ano_mes,
    x.cd_municipio_residencia as municipio,
    COUNT(*) AS numero_obitos
FROM sim.sim_bi x
WHERE x.cd_cid_causa_basica IN ('C340', 'C341', 'C342', 'C343', 'C344', 'C345', 'C346','C347', 'C348', 'C349')
GROUP BY TO_CHAR(x.dt_obito, 'YYYY-MM'), x.cd_municipio_residencia
ORDER BY TO_CHAR(x.dt_obito, 'YYYY-MM'), x.cd_municipio_residencia;
"""

cursor1.execute(consulta1)

resultados1 = cursor1.fetchall()

data8 = pd.DataFrame(resultados1, columns=[desc[0] for desc in cursor1.description])

print(data8)

def project_macroregion(data8, columns_to_project1, start_month, num_months):
    unique_macroregions = data8['municipio'].unique()
    projections = []

    for macroregion in unique_macroregions:
        data_subset = data8[data8['municipio'] == macroregion]

        if data_subset.empty or 'ano_mes' not in data_subset.columns:
            continue

        data_subset = data_subset.sort_values(by='ano_mes')

        for col in columns_to_project1:
            series = data_subset[col]
            current_series = series.copy()  

            for i in range(num_months):
                try:
                    current_date = datetime.now()
                    current_year = current_date.year
                    current_month = start_month + i
                    if current_month > 12:
                        current_month -= 12
                        current_year += 1

                    next_date = datetime(current_year, current_month, 1)

                    # Ajuste do modelo SARIMA usando a série atual
                    order = (1, 1, 1)
                    seasonal_order = (1, 1, 1, 12)
                    model = SARIMAX(current_series, order=order, seasonal_order=seasonal_order)
                    model_fit = model.fit(disp=False)

                    forecast_value = model_fit.get_forecast(steps=1).predicted_mean.values[0]
                    forecast_value = round(forecast_value, 0)

                    data_subset = data_subset.append({'ano_mes': next_date, 'municipio': macroregion, col: forecast_value}, ignore_index=True)

                    # Atualiza a série atual com o novo valor previsto
                    current_series = current_series.append(pd.Series([forecast_value], index=[next_date]))

                except Exception as e:
                    print(f"Erro ao ajustar o modelo para {macroregion}, mês {current_month}: {str(e)}")

            projections.append(data_subset)

    projections_cancer_pulmao = pd.concat(projections, ignore_index=True)

    return projections_cancer_pulmao

columns_to_project1 = ['numero_obitos']

current_date = datetime.now()
start_month = current_date.month
num_months = 4

data8['ano_mes'] = pd.to_datetime(data8['ano_mes'])

projections_cancer_pulmao = project_macroregion(data8, columns_to_project1, start_month, num_months)




#################### FORMATAÇÃO COLUNAS ########################

projections_infantil['numero_mes'] = pd.to_datetime(projections_infantil['ano_mes'].astype(str)).dt.strftime('%B') 
projections_infantil['ano'] = pd.to_datetime(projections_infantil['ano_mes'].astype(str)).dt.strftime('%Y') 

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


projections_infantil['numero_mes'] = projections_infantil['numero_mes'].map(meses_dict)

data_atual = datetime.now()
mes_atual = data_atual.month
ano_atual = data_atual.year
projections_infantil['previsto'] = ~(projections_infantil['ano_mes'].apply(lambda x: (x.year, x.month) <= (ano_atual, mes_atual)))

projections_infantil['tipo_morte'] = 'Morte infantil < 1 ano'
################################################

projections_investigados['numero_mes'] = pd.to_datetime(projections_investigados['ano_mes'].astype(str)).dt.strftime('%B') 
projections_investigados['ano'] = pd.to_datetime(projections_investigados['ano_mes'].astype(str)).dt.strftime('%Y') 

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

projections_investigados['numero_mes'] = projections_investigados['numero_mes'].map(meses_dict)

data_atual = datetime.now()
mes_atual = data_atual.month
ano_atual = data_atual.year
projections_investigados['previsto'] = ~(projections_investigados['ano_mes'].apply(lambda x: (x.year, x.month) <= (ano_atual, mes_atual)))

projections_investigados['tipo_morte'] = 'Mortes Investigadas'

######################## PROSTATA

projections_cancer_prostata['numero_mes'] = pd.to_datetime(projections_cancer_prostata['ano_mes'].astype(str)).dt.strftime('%B') #adicionar o nome do mes dentro da coluna
projections_cancer_prostata['ano'] = pd.to_datetime(projections_cancer_prostata['ano_mes'].astype(str)).dt.strftime('%Y') #adicionar o nome do ano dentro da coluna

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

projections_cancer_prostata['numero_mes'] = projections_cancer_prostata['numero_mes'].map(meses_dict)

data_atual = datetime.now()
mes_atual = data_atual.month
ano_atual = data_atual.year
projections_cancer_prostata['previsto'] = ~(projections_cancer_prostata['ano_mes'].apply(lambda x: (x.year, x.month) <= (ano_atual, mes_atual)))

projections_cancer_prostata['tipo_morte'] = 'Cancer Prostata'


######################## UTERO

projections_cancer_utero['numero_mes'] = pd.to_datetime(projections_cancer_utero['ano_mes'].astype(str)).dt.strftime('%B') #adicionar o nome do mes dentro da coluna
projections_cancer_utero['ano'] = pd.to_datetime(projections_cancer_utero['ano_mes'].astype(str)).dt.strftime('%Y') #adicionar o nome do ano dentro da coluna

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

projections_cancer_utero['numero_mes'] = projections_cancer_utero['numero_mes'].map(meses_dict)

data_atual = datetime.now()
mes_atual = data_atual.month
ano_atual = data_atual.year
projections_cancer_utero['previsto'] = ~(projections_cancer_utero['ano_mes'].apply(lambda x: (x.year, x.month) <= (ano_atual, mes_atual)))

projections_cancer_utero['tipo_morte'] = 'Cancer Utero'

######################## MAMA

projections_cancer_mama['numero_mes'] = pd.to_datetime(projections_cancer_mama['ano_mes'].astype(str)).dt.strftime('%B') #adicionar o nome do mes dentro da coluna
projections_cancer_mama['ano'] = pd.to_datetime(projections_cancer_mama['ano_mes'].astype(str)).dt.strftime('%Y') #adicionar o nome do ano dentro da coluna

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

projections_cancer_mama['numero_mes'] = projections_cancer_mama['numero_mes'].map(meses_dict)

data_atual = datetime.now()
mes_atual = data_atual.month
ano_atual = data_atual.year
projections_cancer_mama['previsto'] = ~(projections_cancer_mama['ano_mes'].apply(lambda x: (x.year, x.month) <= (ano_atual, mes_atual)))

projections_cancer_mama['tipo_morte'] = 'Cancer Mama'

######################## PULMAO

projections_cancer_pulmao['numero_mes'] = pd.to_datetime(projections_cancer_pulmao['ano_mes'].astype(str)).dt.strftime('%B') #adicionar o nome do mes dentro da coluna
projections_cancer_pulmao['ano'] = pd.to_datetime(projections_cancer_pulmao['ano_mes'].astype(str)).dt.strftime('%Y') #adicionar o nome do ano dentro da coluna

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

projections_cancer_pulmao['numero_mes'] = projections_cancer_pulmao['numero_mes'].map(meses_dict)

data_atual = datetime.now()
mes_atual = data_atual.month
ano_atual = data_atual.year
projections_cancer_pulmao['previsto'] = ~(projections_cancer_pulmao['ano_mes'].apply(lambda x: (x.year, x.month) <= (ano_atual, mes_atual)))

projections_cancer_pulmao['tipo_morte'] = 'Cancer Pulmão'

#########################
resultado1 = pd.concat([projections_cancer_utero, projections_cancer_pulmao], axis=0)
resultado2 = pd.concat([resultado1, projections_cancer_mama], axis=0)
resultado3 = pd.concat([resultado2, projections_cancer_prostata], axis=0)
resultado4 = pd.concat([resultado3, projections_infantil], axis=0)
resultado_final = pd.concat([resultado4, projections_investigados], axis=0)


resultado_final['numero_obitos'] = resultado_final['numero_obitos'].astype(int)

# ################## CARREGAR NO BANCO DE DADOS ###########################

resultado_final = resultado_final[resultado_final['previsto'] == True]


import psycopg2

conn = psycopg2.connect(
    database="******",
    user="******",
    password="******",
    host="******",
    port="****"
)
try:
    cursor = conn.cursor()
    conn.autocommit = False
    cursor.execute('TRUNCATE TABLE projetados.pri_projecoes_mortalidades')
    for index, row in resultado_final.iterrows():
        cursor.execute(
            'INSERT INTO projetados.pri_projecoes_mortalidades '
            '("ano_mes", "municipio", "numero_obitos", "numero_mes", "ano", "previsto", "tipo_morte") '
            'VALUES (%s, %s, %s, %s, %s, %s, %s);',
            (
                row["ano_mes"],
                row["municipio"],
                row["numero_obitos"],
                row["numero_mes"],
                row["ano"],
                row["previsto"],
                row["tipo_morte"]
            )
        )
    conn.commit()
except Exception as e:
    conn.rollback()
    print("Erro:", e)
finally:
    cursor.close()
    conn.close()

