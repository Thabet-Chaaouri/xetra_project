import logging
import logging.config
import yaml


def main():
    # Init
    config_path = "C:/Users/thabe/xetra_project/xetra_project/configs/xetra_report1_config.yml"
    config = yaml.safe_load(open(config_path))

    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("this is a test")
if __name__ == '__main__':
    main()
