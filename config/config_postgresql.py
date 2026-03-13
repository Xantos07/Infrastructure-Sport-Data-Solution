from configparser import ConfigParser
from pathlib import Path

def load_config(filename='database.ini', section='postgresql'):
    config_path = Path(filename)
    if not config_path.is_absolute() and not config_path.exists():
        config_path = Path(__file__).resolve().parent / filename

    parser = ConfigParser()
    parser.read(config_path)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, config_path))

    return config

if __name__ == '__main__':
    config = load_config()
    print(config)