import os
from configparser import ConfigParser

def load_config(file_name='database.ini', section='sqlserver'):
    script_dir = os.path.dirname(__file__)  # Lấy thư mục chứa script hiện tại
    file_path = os.path.join(script_dir, file_name)
    parser = ConfigParser()
    parser.read(file_path)

    #get section, default to sqlserver
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