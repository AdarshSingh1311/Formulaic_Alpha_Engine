from all_general_imports import *

def days_above_ema (df) :
    lb = 100
    df['ema'] = ta.ema(df.close,length= lb)
    df = df[lb:].copy()

    #df['score'] = (df.close - df.ema)/df.close
    df['score'] = np.where( (df.close > df.ema) & (df.close.shift(1) < df.ema.shift(1)) , 1 , 0) 
    df['score'] = np.where( (df.close < df.ema) & (df.close.shift(1) > df.ema.shift(1)) , -1 , df.score) 
    def length_ind (df_) :
        signal_df_ = df_[(df_.score == 1) | (df_.score == -1) ] 
        if signal_df_.empty : return 0

        last_signal = signal_df_.score.iloc[-1]
        if last_signal == -1 :
            return 0
        elif last_signal == 1 :
            indx_past = signal_df_.index[-1]
            return len(df_.loc[indx_past:])
    
    df['score'] = [ length_ind(df.loc[:indx]) for indx in df.index  ]   

    return df.score

def above_ema (df) :
    lb = 100
    df['ema'] = ta.ema(df.close,length= lb)
    df = df[lb:].copy()

    df['score'] = (df.close - df.ema)/df.close

    return df.score




