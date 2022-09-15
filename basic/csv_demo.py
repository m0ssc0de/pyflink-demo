from pyflink.table import (TableEnvironment, EnvironmentSettings, 
                           DataTypes,  CsvTableSource)

def main():
    env_settings = EnvironmentSettings.new_instance()\
                                      .in_batch_mode()\
                                      .build()
                                    #   .use_blink_planner()\
    tbl_env = TableEnvironment.create(env_settings)

    column_names = ['trx_id', 'trx_date', 'src_curr', 'amnt_src_curr', 
                    'amnt_gbp', 'user_id', 'user_type', 'user_country']
    
    column_types = [DataTypes.INT(), DataTypes.DATE(), DataTypes.STRING(), DataTypes.DOUBLE(), 
                    DataTypes.DOUBLE(), DataTypes.INT(),DataTypes.STRING(), DataTypes.STRING()]
    
    source = CsvTableSource(
        './csv_source.csv',
        column_names,
        column_types,
        ignore_first_line=False
    )
    
    tbl_env.register_table_source('financial_trxs', source)
    
    tbl = tbl_env.from_path('financial_trxs')
    
    ##############################
    print('\nRegistered Tables List')
    print(tbl_env.list_tables())

    print('\nFinancial Trxs Schema')
    tbl.print_schema()

    print('\nFinancial Trxs Data')
    print(tbl.to_pandas()) 
    
    #or tbl.limit(10).execute().print()
    
if __name__ == '__main__':
    main()