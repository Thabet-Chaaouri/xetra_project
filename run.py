"""Running the Xetra ETL application"""
import argparse
import logging
import logging.config

import yaml

from xetra.common.s3 import S3BucketConnector
from xetra.transformers.xetra_transformer import XetraETL, XetraSourceConfig, XetraTargetConfig


def main():
    """
        entry point to run the xetra ETL job.
    """
    # Parsing YML file
    parser = argparse.ArgumentParser(description='Run the Xetra ETL Job.')
    parser.add_argument('config', help='A configuration file in YAML format.')
    args = parser.parse_args()

#    config = 'C:/Daten/xetra_project_old/xetra_1234/configs/xetra_report1_config.yaml'
#    config = yaml.safe_load(open(config))

    config = yaml.safe_load(open(args.config))
    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    # reading s3 configuration
    s3_config = config['s3']
    # creating the S3BucketConnector classes for source and target
    s3_bucket_src = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['src_endpoint_url'],
                                      bucket=s3_config['src_bucket'])
    s3_bucket_trg = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['trg_endpoint_url'],
                                      bucket=s3_config['trg_bucket'])
    # reading source configuration
    source_config = XetraSourceConfig(**config['source'])
    # reading target configuration
    target_config = XetraTargetConfig(**config['target'])
    # reading meta file configuration
    meta_config = config['meta']
    # creating XetraETL class
    logger = logging.getLogger(__name__)
    logger.info('Xetra ETL job started.')
    xetra_etl = XetraETL(s3_bucket_src, s3_bucket_trg,
                         meta_config['meta_key'], source_config, target_config)
    # running etl job for xetra report 1
    xetra_etl.etl_report1()
    logger.info('Xetra ETL job finished.')


if __name__ == '__main__':
    main()
