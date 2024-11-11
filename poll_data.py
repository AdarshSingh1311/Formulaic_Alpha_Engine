try :
    from session_init import breeze
    from all_general_imports import *
except :
    from .session_init import breeze
    from .all_general_imports import *


import threading


def get_stock_eq_data(stock,from_,now_,granularity,eq_db_bool = False) :
    
    def clean_parse_data (data_df) :
        col = ['open','high', 'low', 'close']
        data_df = data_df.set_index('datetime',drop = True)
        data_df[col] = data_df[col].astype(float)
        return data_df[col]
    
    try :
        json = breeze.get_historical_data(interval=granularity,
                                        from_date=  from_,
                                        to_date= now_,
                                        stock_code= stock,
                                        exchange_code="NSE",
                                        product_type="cash")

        data_df= pd.DataFrame(json['Success'])

        if data_df.empty : return
        print(stock)

    except Exception as e :
        print(e)
        print(json)
        return

    if not eq_db_bool  : return data_df
    
    eq_db[stock] = clean_parse_data(data_df)


def eq_database(insts,max_api_limit_per_min = 100) :

    slice = int((len(insts)/max_api_limit_per_min) + 1)
    chunks = int(len(insts)/slice)
    indxes = np.arange(0,len(insts),chunks)[:slice]
    
    chunks_insts = []
    for indx in range(len(indxes)) :
        if indx < len(indxes)-1 : chunks_insts.append(insts[indxes[indx]:indxes[indx+1]]) 
        else : chunks_insts.append(insts[indxes[indx]:])    

    for chunk_insts in chunks_insts :

        print(chunks_insts)
        insts = chunk_insts
        threads = [threading.Thread(target=get_stock_eq_data,args=(inst,from_,now_,granularity,True)) for inst in insts ]
        for thread in threads : thread.start()
        for thread in threads : thread.join()
        
        if chunks_insts[-1] == chunk_insts : return eq_db
        print('100 API CALLS IN A MINUTE LIMIT REACHED')
        print('---WAITING 1 MINUTE TO PASS --')
        time.sleep(60)
         
    return eq_db
   
def get_eq_insts (type = 'all_eq' or 'fno')  :

    if type == 'all_eq' :
        def clean_df_col (df) :
            clean_col_name = lambda x : x.split('"')[1] if '"' in x else x
            df.columns = list(map( clean_col_name,  df.columns ))

        df = pd.read_csv('NSEScripMaster.txt')
        clean_df_col(df)
        df = df[df['Series'] == 'EQ'].reset_index(drop = True)
        return list(df.ShortName.iloc[250:280])
    
    if type == 'fno' :
        df = pd.read_excel('icici stk name name.xlsx')
        return df[1:]['Stock Code'].to_list()


def get_eq_db() :
    try :
        with open('eq_db.pickle' , 'rb') as file :
            return pickle.load(file) 
    except :
        print('erorr loading eq database') 

trail_dt = lambda x : x + "T07:00:00.000Z" if isinstance(x,str) else str(x) + "T07:00:00.000Z"        


if __name__ == '__main__' :

    now_ = datetime.datetime.now().date()
    from_ = now_ - timedelta(days = 365*10)
    from_,now_ = trail_dt(from_),trail_dt(now_)
    granularity = '1day'
    eq_db = {}
    insts = get_eq_insts(type = 'fno')
    eq_db = eq_database(insts)
    
    print(eq_db)
    with open('eq_db.pickle' , 'wb') as file :
        pickle.dump(eq_db,file)           
   

#print(eq_db)
#print(eq_db.keys())
#print(len(eq_db))
#print(insts)
#print(len(insts))