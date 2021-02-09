import xml.etree.ElementTree as element_tree
import pandas
import fnmatch
import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

#nazwa pliku do zmiennej `source_file_old_name`
source_file = []
for file_name in os.listdir('<DIR>'):
    if fnmatch.fnmatch(file_name, 'Rejestr*.xml'):
        source_file.append(file_name)
source_file_old_name = str(source_file[0])

#nowa nazwa pliku, `source_file_new_name` zostanie użyta po załadowaniu do bazy do metody os.rename
source_file_new_name = str('old_{}'.format(source_file[0]))


file_path = os.path.join('<DIR>', source_file_old_name)
#print(file_path)

##########################parse XML##########################
#root node
root = element_tree.parse(file_path).getroot()

#tworzenie słownika namespace
name_space = {node[0]: node[1] for _, node in element_tree.iterparse(file_path, events=['start-ns'])}
for key, value in name_space.items():   
    element_tree.register_namespace(key, value)
#print(name_space)

#wyciągamy pole 'Apteki.stanNaDzien' oraz wartość do osobnych zmiennych
data_frame_root = [{**{f"{d.tag.split('}')[1]}_{k}":v for k,v in d.items()}}
 for d in root.findall(".", name_space)]
report_date_attr = list(data_frame_root[0].keys())[0]
report_date = list(data_frame_root[0].values())[0]

#parsowanie: oparte o namespace `  '': 'http://rejestrymedyczne.csioz.gov.pl/ra/eksport-danych-v1.0' `
#dlatego w ścieżkach do node'ów nie ma klucza bo jest on ''
#transformacja do dataframe, oparta o ** oraz interpolację stringów f'{} z listami i słownikami
#wybór tylko kilku kolumn, udało się dodać prefix do każdego z child node, na razie bez root

data_frame = pandas.DataFrame([{**{f"{d.tag.split('}')[1]}_{k}":v for k,v in d.items()}, 
  **{f"{co.tag.split('}')[1]}_{k}":v  for k,v in co.items()}, 
  **{f"{addr.tag.split('}')[1]}_{k}":v for addr in co.findall("Adres", name_space) for k,v in addr.items()} }
 for d in root.findall("Apteka", name_space)
 for co in d.findall("Wlasciciele/Wlasciciel", name_space)
], columns = ['Apteka_id', 'Apteka_rodzaj', 'Apteka_status', 'Apteka_nazwa', 'Wlasciciel_nazwa', 'Wlasciciel_nip'])


#dostawiamy kolumny
data_frame[report_date_attr] = report_date
data_frame['etl_filename'] = source_file[0]  
data_frame['etl_timestamp'] = pandas.to_datetime('now').replace(microsecond=0)

##########################parse XML, koniec##########################



#connect to snowflake #1
snowflake_connect = snowflake.connector.connect(
    user='',
    password='',
    account='',
    database='',
    schema='')
snowflake_connect_cursor = snowflake_connect.cursor()
try:
    snowflake_connect_cursor.execute("SELECT current_version()")
    one_row = snowflake_connect_cursor.fetchone()
    print("Connection ok, Snowflake version: " + str(one_row[0]))

finally:
    #zapis do STAGE
    write_pandas(snowflake_connect, data_frame, 'STAGE_REJESTR_APTEK', quote_identifiers=False)
    print("data loaded to STAGE_REJESTR_APTEK!")
    snowflake_connect_cursor.close()
snowflake_connect.close()


#connect to snowflake #2
snowflake_connect = snowflake.connector.connect(
    user='',
    password='',
    account='',
    database='',
    schema='')
snowflake_connect_cursor = snowflake_connect.cursor()
try:
    snowflake_connect_cursor.execute("SELECT current_version()")
    one_row = snowflake_connect_cursor.fetchone()
    print("Connection ok, Snowflake version: " + str(one_row[0]))

finally:
    snowflake_connect_cursor.execute("""CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.T_REJESTR_APTEK
                                        as
                                        SELECT 
                                            TO_NUMBER(APTEKA_ID) as DRUGSTORE_ID,
                                            APTEKA_RODZAJ as DRUGSTORE_TYPE,
                                            APTEKA_STATUS as DRUGSTORE_ITEM_STATUS,
                                            APTEKA_NAZWA as DRUGSTORE_NAME,
                                            WLASCICIEL_NAZWA as DRUGSTORE_OWNER_NAME,
                                            WLASCICIEL_NIP as DRUGSTORE_TIN,
                                            APTEKI_STANNADZIEN as FILE_TIMESTAMP,
                                            TO_TIMESTAMP(ETL_TIMESTAMP) as ETL_TIMESTAMP
                                        FROM DEMO_DB.PUBLIC.STAGE_REJESTR_APTEK;""") 
    print("data loaded to T_REJESTR_APTEK!")
    snowflake_connect_cursor.close()
snowflake_connect.close()

#zmienne do os.rename
source_file_path = os.path.join('<DIR>', source_file_old_name)
source_file_path_renamed = os.path.join('<DIR>', source_file_new_name)

#zmiana nazwy pliku
os.rename(source_file_path, source_file_path_renamed)
print('source file renamed')

#zapis do pliku
#print(data_frame)
#data_frame.to_csv('<DIR>/Filename.csv')                                