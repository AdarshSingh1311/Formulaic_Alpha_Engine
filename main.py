from all_general_imports import *
from poll_data import get_eq_db
from alphas import above_ema

def stich_insts (col,custom_func = None,custom_indx = None) :
    global eq_db
    scores = []
    for inst in eq_db.keys() :
        print(inst)
        if custom_func is None : 
            if col == '%' : ser = eq_db[inst]['close'].pct_change().bfill()*100
            else : ser = eq_db[inst][col] 
        else : 
            df = eq_db[inst]
            #else : df = eq_db[inst].loc[custom_indx]
            ser = custom_func(df)

        scores.append(ser) 

    scores_df = pd.concat(scores,axis = 1) 
    scores_df.columns = eq_db.keys() 
    scores_df.index =  [ pd.to_datetime(indx).date() for indx in scores_df.index ]
    scores_df.sort_index(inplace= True)

    if custom_indx is not None : return scores_df.loc[custom_indx]
    return scores_df 


def stocks_with_cor_actions (insts_close) :
    cor_actions = []
    for inst in insts_close.columns :
        df = insts_close[inst]
        if np.any((df < 0.5*df.shift(1)) | (df > 1.5*df.shift(1)) ) : cor_actions.append(inst)

    return cor_actions 


def rebalnce (df_signals,weekday_int =  0) :
    weekday_int_map = {}
    orignal_index = df_signals.index
    df_signals['weekday'] = [ indx.weekday() for indx in df_signals.index ]
    df = df_signals[df_signals['weekday'] == weekday_int ]
    reb_df = pd.DataFrame(index= orignal_index)
    reb_df = reb_df.join(df)
    reb_df.ffill(inplace=True)
    reb_df.drop(columns='weekday',inplace=True)

    return reb_df

def rank_constant_inst(row) :
    row_ = row[row >= rank_long(row)].sort_values()
    ranked_insts = row_.iloc[-25:].index
    
    lst = []
    for row_index in row.index :
        if row_index in ranked_insts  :
            lst.append(1)
        else :
            lst.append(0)    
    
    row = pd.Series(lst,index = row.index)
    return row 


if __name__ == '__main__' :

    eq_db = get_eq_db()

    insts_close = stich_insts('close')
    insts_close = insts_close.ffill().dropna()
    print(insts_close)

    cor_actions = stocks_with_cor_actions(insts_close)
    print(cor_actions)
    
    # ALPHA
    score = above_ema
    
    score_df = stich_insts(col = None ,custom_func=score,custom_indx=insts_close.index)

    #remove stocks with cor_actions
    score_df.drop(columns= cor_actions , inplace= True)
    
    rank_long = lambda x : x.sort_values().to_list()[-25]
    rank_short = lambda x : x.sort_values().to_list()[24]              

    signal_df = score_df.apply( lambda row : (row >= rank_long(row)).astype(int), axis = 1)
    #signal_df = score_df.apply( lambda row : rank_constant_inst(row), axis = 1)
    signal_df = signal_df.shift(1).bfill() 

    # rebalance weekly 
    signal_df = rebalnce(signal_df)

    ret_df = stich_insts(col = '%',custom_indx=insts_close.index)

    strat_df = ret_df*signal_df
    pnl = (strat_df.sum(axis = 1)/25).cumsum()
    pnl.to_csv('pnl1.csv')
    signal_df.to_csv('under_index.csv')
