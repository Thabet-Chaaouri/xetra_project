"""
Methods for processing the metafile
"""

class MetaProcess():
    @staticmethod
    def update_meta_file(bucket, meta_key, extract_date_list):
        df_new = pd.DataFrame(columns=['source_date', 'datetime_of_processing'])
        df_new['source_date'] = extract_date_list
        df_new['datetime_of_processing'] = datetime.today().strftime('%Y-%m-%d')
        df_old = read_csv_to_df(bucket, meta_key)
        df_all = pd.concat([df_old, df_new])
        write_df_to_s3_csv(bucket, df_all, meta_key)
    @staticmethod
    def return_date_list(bucket, arg_date, src_format, meta_key):
        min_date = datetime.strptime(arg_date, src_format).date() - timedelta(days=1)
        today = datetime.today().date()
        try:
            df_meta = read_csv_to_df(bucket, meta_key)
            dates = [(min_date + timedelta(days=x)) for x in range(0, (today - min_date).days + 1)]
            src_dates = set(pd.to_datetime(df_meta['source_date']).dt.date)
            dates_missing = set(dates[1:]) - src_dates
            if dates_missing:
                min_date = min(set(dates[1:]) - src_dates) - timedelta(days=1)
                return_dates = [date.strftime(src_format) for date in dates if date >= min_date]
                return_min_date = (min_date + timedelta(days=1)).strftime(src_format)
            else:
                return_dates = []
                return_min_date = datetime(2200, 1, 1).date()
        except bucket.session.client('s3').execptions.NoSuchKey:
            return_dates = [(min_date + timedelta(days=x)).strftime(src_format) for x in
                            range(0, (today - min_date).days + 1)]
            return_min_date = arg_date
        return return_min_date, return_dates


