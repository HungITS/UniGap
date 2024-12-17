from configparser import ConfigParser

def load_config(file_name='/home/hungits/Documents/UniGap/Kickoff/PostgresSQL_Python/database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(file_name)

    #get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, file_name))

    return config

if __name__ == '__main__':
    config = load_config()
    print(config)