# xml_2_snowflake
This code will allow to parse and load xml file (https://rejestrymedyczne.ezdrowie.gov.pl/registry/ra) to Snowflake database.
Parsing is combination of args/kwargs with string interpolation and list comprehension.

Project is divided into 3 modules when combined recreates the ELT pattern of data loading.

Run the code using: `python3 function.py <source_file_dir> Rejestr*.xml`
