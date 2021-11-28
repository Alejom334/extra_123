'''
Running the Xetra ETL application
'''
import logging
import logging.config as config
import yaml

'''
Purpose:
    Entry point to run rhe extra ETL job
'''
def main():
    #Parsing YAML file
    config_path = '/Users/alejandromartinez/PycharmProjects/etlpipeline/xetra_123/configs/xetra_report1_config.yml'
    config = yaml.safe_load(open(config_path))
    print(config)

    #Configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is a test.")


if __name__ == '__main__':
    main()

