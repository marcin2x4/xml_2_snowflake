import file_get
import xml.etree.ElementTree as element_tree
import pandas
import sys
import db
from db import ConnectionError, CredentialsError, SQLError
import datetime
from functools import wraps
import time

def extra_attrs(func):
    @wraps(func)
    def new_fun(*args, **kwargs):
        print('Arguments are: ' , args)
        print('Key-word arguments are: ' , kwargs)
        func(*args, **kwargs)
    return new_fun

@extra_attrs
def logger(**kwargs):
    print("just logging stuff..")
    print(kwargs["date"])


def time_note(func):
    @wraps(func)
    def time_note_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time-start_time
        print(f'{func.__name__}{args} {kwargs} took {total_time:.4f} seconds')
        return result
    return time_note_wrapper

@time_note
def main():

    dir = sys.argv[1]
    file_input = sys.argv[2]

    file_path = file_get.FileGet(dir, file_input)

    try:
        path, file, directory = file_path.get_source_file_path()
        root = element_tree.parse(path).getroot()

        name_space = {
            node[0]: node[1]
            for _, node in element_tree.iterparse(path, events=["start-ns"])
        }
        for key, value in name_space.items():
            element_tree.register_namespace(key, value)

        data_frame_root = [
            {**{f"{d.tag.split('}')[1]}_{k}": v for k, v in d.items()}}
            for d in root.findall(".", name_space)
        ]
        report_date_attr = list(data_frame_root[0].keys())[0]
        report_date = list(data_frame_root[0].values())[0]

        data_frame = pandas.DataFrame(
            [
                {
                    **{f"{d.tag.split('}')[1]}_{k}": v for k, v in d.items()},
                    **{f"{co.tag.split('}')[1]}_{k}": v for k, v in co.items()},
                    **{
                        f"{addr.tag.split('}')[1]}_{k}": v
                        for addr in co.findall("Adres", name_space)
                        for k, v in addr.items()
                    },
                }
                for d in root.findall("Apteka", name_space)
                for co in d.findall("Wlasciciele/Wlasciciel", name_space)
            ],
            columns=[
                "Apteka_id",
                "Apteka_rodzaj",
                "Apteka_status",
                "Apteka_nazwa",
                "Wlasciciel_nazwa",
                "Wlasciciel_nip",
            ],
        )

        data_frame[report_date_attr] = report_date
        data_frame["etl_filename"] = file
        data_frame["etl_timestamp"] = pandas.to_datetime("now").replace(microsecond=0)

        # print(data_frame)

        file_path.file_rename(file, directory)

        with db.UseDB() as cursor:
            cursor.execute("SELECT current_version()").fetchone()

            db.UseDB().frame_writer(data_frame, "STAGE_REJESTR_APTEK", False)

            cursor.execute(
                """MERGE INTO DEMO_DB.PUBLIC.TABLE_REJESTR_APTEK t USING DEMO_DB.PUBLIC.STAGE_REJESTR_APTEK s 
                              ON t.DRUGSTORE_AS_OF_DATE = s.APTEKI_STANNADZIEN

                              WHEN NOT MATCHED THEN
                                  INSERT (DRUGSTORE_ID, DRUGSTORE_TYPE, DRUGSTORE_ITEM_STATUS, DRUGSTORE_NAME, DRUGSTORE_OWNER_NAME, DRUGSTORE_OWNER_NIP, DRUGSTORE_AS_OF_DATE, ETL_FILENAME, ETL_TIMESTAMP) 
                                  VALUES (TO_NUMBER(s.APTEKA_ID), s.APTEKA_RODZAJ, s.APTEKA_STATUS, s.APTEKA_NAZWA, s.WLASCICIEL_NAZWA, s.WLASCICIEL_NIP, s.APTEKI_STANNADZIEN, s.ETL_FILENAME, TO_TIMESTAMP(s.ETL_TIMESTAMP));"""
            )

            cursor.execute(
                "TRUNCATE TABLE DEMO_DB.PUBLIC.STAGE_REJESTR_APTEK;"
            ).fetchone()

        file_path.file_rename(file, directory)

    except file_get.FileNotFoundError as e:
        print(e)
    except file_get.FileExistsError as e:
        print(e)
    except ConnectionError as err:
        print("Connection error: ", str(err))
    except CredentialsError as err:
        print("Creds error: ", str(err))
    except SQLError as err:
        print("Query error: ", str(err))
    except Exception as err:
        print("Some other issue occured:", str(err))
    return "Error"


if __name__ == "__main__":
    logger(date=datetime.date.today())
    main()
    print(logger.__name__)
    print(logger.__doc__)
    print(main.__name__)
    print(main.__doc__)