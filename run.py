""" Running the Xetra ETL application"""
import logging
import logging.config
import yaml


def main():
    """
    Entery point to run xetra ETL job
    :return:
    """
    # Parsing YAML file
    config_parth = 'configs/xetra_report1_config.yml'
    config = yaml.safe_load(open(config_parth))
    # Configure logging
    log_config = config["logging"]
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is a test.")


if __name__ == '__main__':
    main()
