from pyflink.table import (TableEnvironment, EnvironmentSettings, 
                           DataTypes,  CsvTableSource)

def main():
    env_settings = EnvironmentSettings.new_instance()\
                                      .in_batch_mode()\
                                      .build()
                                    #   .use_blink_planner()\
    tbl_env = TableEnvironment.create(env_settings)

    column_names = ['hash','nonce','block_hash','block_number','transaction_index','from_address','to_address','value','gas','gas_price','input','block_timestamp','max_fee_per_gas','max_priority_fee_per_gas','transaction_type']
    # 0xcecf1e0adb943fcd8fbd5f6ecd27d16c028d5b26f6ac7cbe4e8962698cca87d6,4,0xaf0615219cf8b66cabdd0ca559cc27dfc070740489f5f83fd7afcdf717d00ee4,60003,0,0xc56e71225f1372c990e6829d53a1ab76d27301cf,0xa5fb3e4399e8f18643f969d0ce65cc352fc252c6,223000000000000000000,90000,60442654402,0x,1439147361,,,0
    
    column_types = [DataTypes.STRING(), DataTypes.INT(), DataTypes.STRING(), DataTypes.INT(), 
                    DataTypes.INT(), DataTypes.STRING(),DataTypes.STRING(), DataTypes.DOUBLE(), DataTypes.DOUBLE(),DataTypes.DOUBLE(),DataTypes.STRING(),DataTypes.INT(),DataTypes.DOUBLE(),DataTypes.DOUBLE(),DataTypes.STRING()]
    
    source = CsvTableSource(
        './transactions.csv',
        column_names,
        column_types,
        ignore_first_line=True
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