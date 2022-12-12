from configparser import ConfigParser

def get_db_info(filename,section):
    parser=ConfigParser()
    parser.read(filename)

    db_info={}
    if parser.has_section(section):
         key_val_tuple = parser.items(section)
         for item in key_val_tuple:
             db_info[item[0]]=item[1]

    return db_info
