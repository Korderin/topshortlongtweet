import pdfplumber
import pandas as pd
import requests
from datetime import datetime, timedelta
import tweepy

api_key = 
api_secret = 
bearer_token =
access_token = 
access_token_secret = 
client = tweepy.Client(bearer_token, api_key, api_secret, access_token, access_token_secret)

def descargar_pdf(fecha):
    url = f"https://cibe.bolsadesantiago.com/Documentos/EstadisticasyPublicaciones/Boletines%20Burstiles/ibd{fecha}.pdf"
    response = requests.get(url)
    if response.status_code == 200:
        pdf_path = f"ibd{fecha}.pdf"
        with open(pdf_path, "wb") as file:
            file.write(response.content)
        print(f"Archivo ibd{fecha}.pdf descargado correctamente.")
        return pdf_path
    else:
        raise Exception(f"No se pudo descargar el archivo para la fecha {fecha}.")

# Fecha del día anterior en formato ddmmyy
fecha_ayer = (datetime.now() - timedelta(days=1)).strftime("%d%m%y")
pdf_path = descargar_pdf(fecha_ayer)

# Abre el archivo PDF
with pdfplumber.open(pdf_path) as pdf:
    # Actualizamos las páginas para las operaciones simultáneas y ventas cortas
    page_14 = pdf.pages[13]
    tables_14 = page_14.extract_tables()

    page_15 = pdf.pages[14]
    tables_15 = page_15.extract_tables()

    # Ventas cortas están en las páginas 15 y 16
    tables_16 = []
    if len(pdf.pages) > 15:
        page_16 = pdf.pages[15]
        tables_16 = page_16.extract_tables()

# Verifica que se hayan extraído tablas en las páginas necesarias
if not tables_14 or len(tables_14) == 0:
    raise ValueError("No se encontraron tablas en la página 14 del PDF")
if not tables_15 or len(tables_15) == 0:
    raise ValueError("No se encontraron tablas en la página 15 del PDF")

# Procesa las tablas de operaciones simultáneas y ventas cortas
if len(tables_15) == 1:
    tabla_os = tables_15[0]
    tabla_ventas_cortas = tables_16[0] if tables_16 else None
else:
    tabla_os = tables_15[0]
    tabla_ventas_cortas = tables_15[1]

# Crea DataFrames de pandas con las tablas extraídas
df_os_14 = pd.DataFrame(tables_14[0][1:], columns=tables_14[0][0])
df_os_15 = pd.DataFrame(tabla_os[1:], columns=tabla_os[0])

# Combina los DataFrames de las tablas de operaciones simultáneas de ambas páginas
df_os_total = pd.concat([df_os_14, df_os_15])

# Procesa los datos de operaciones simultáneas
if 'Nemo' in df_os_total.columns:
    df_os_total[['Nemo', 'Volumen_OS', 'Monto_OS']] = df_os_total['Nemo'].str.split(' ', n=2, expand=True)
    df_os_total = df_os_total[['Nemo', 'Volumen_OS', 'Monto_OS']]
    df_os_total['Volumen_OS'] = pd.to_numeric(df_os_total['Volumen_OS'].str.replace('.', '').str.replace(',', '.'), errors='coerce')
    df_os_total['Monto_OS'] = pd.to_numeric(df_os_total['Monto_OS'].str.replace('.', '').str.replace(',', '.'), errors='coerce')
    df_os_total.set_index('Nemo', inplace=True)
    df_os_filtrado = df_os_total[~df_os_total.index.str.startswith('TP')]
    df_os_ordenado = df_os_filtrado.sort_values(by='Monto_OS', ascending=False)
    top_5_simultaneas = df_os_ordenado.head(3).copy()
    top_5_simultaneas['Monto_OS'] = top_5_simultaneas['Monto_OS'].apply(lambda x: f"${x:,.0f}".replace(",", "."))
    
    # Reemplaza los nombres de Nemo con hashtags
    top_5_simultaneas.index = top_5_simultaneas.index.to_series().apply(lambda x: f"#{x}")
    print("\nTop 3 acciones con mayores operaciones simultáneas:")
    print(top_5_simultaneas[['Monto_OS']])
else:
    print("La columna 'Nemo' no está presente en el DataFrame de operaciones simultáneas.")

# Procesa las tablas de ventas cortas
df_ventas_cortas = pd.DataFrame()

# Agrega la segunda tabla de la página 15 si existe
if len(tables_15) > 1:
    df_ventas_cortas_15 = pd.DataFrame(tables_15[1][1:], columns=tables_15[1][0])
    df_ventas_cortas = pd.concat([df_ventas_cortas, df_ventas_cortas_15])

# Agrega las tablas de la página 16 si existen
if tables_16:
    for table in tables_16:
        df_temp = pd.DataFrame(table[1:], columns=table[0])
        df_ventas_cortas = pd.concat([df_ventas_cortas, df_temp])

if 'Nemo' in df_ventas_cortas.columns:
    df_ventas_cortas[['Nemo', 'Volumen_VC', 'Monto_VC']] = df_ventas_cortas['Nemo'].str.split(' ', n=2, expand=True)
    df_ventas_cortas = df_ventas_cortas[['Nemo', 'Volumen_VC', 'Monto_VC']]
    df_ventas_cortas['Volumen_VC'] = pd.to_numeric(df_ventas_cortas['Volumen_VC'].str.replace('.', '').str.replace(',', '.'), errors='coerce')
    df_ventas_cortas['Monto_VC'] = pd.to_numeric(df_ventas_cortas['Monto_VC'].str.replace('.', '').str.replace(',', '.'), errors='coerce')
    df_ventas_cortas.set_index('Nemo', inplace=True)
    df_ventas_cortas_ordenado = df_ventas_cortas.sort_values(by='Monto_VC', ascending=False)
    top_5_ventas_cortas = df_ventas_cortas_ordenado.head(3).copy()
    top_5_ventas_cortas['Monto_VC'] = top_5_ventas_cortas['Monto_VC'].apply(lambda x: f"${x:,.0f}".replace(",", "."))
    
    # Reemplaza los nombres de Nemo con hashtags
    top_5_ventas_cortas.index = top_5_ventas_cortas.index.to_series().apply(lambda x: f"#{x}")
    print("\nTop 3 acciones con mayores ventas cortas:")
    print(top_5_ventas_cortas[['Monto_VC']])
else:
    print("La columna 'Nemo' no está presente en el DataFrame de ventas cortas.")

# Publicar tweets
fechatweet = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")
textoOperacionesSimultaneas = f"Top 3 acciones con mayores operaciones simultáneas el día {fechatweet}:\n" + "\n".join(top_5_simultaneas.index + " " + top_5_simultaneas['Monto_OS'])
textoVentasCortas = f"Top 3 acciones con mayores ventas cortas el día {fechatweet}:\n" + "\n".join(top_5_ventas_cortas.index + " " + top_5_ventas_cortas['Monto_VC'])
client.create_tweet(text=textoOperacionesSimultaneas)
client.create_tweet(text=textoVentasCortas)
