from StockXDict import sneakers, columns
import os
import pandas as pd
import requests

def scrape(productID):
    url = 'https://stockx.com/api/v1/products/' + productID + '/activity?state=480' 
    page = requests.get(url)
    data = page.text
    df = pd.read_json(data)
    try: 
       df = df[columns]
    except:
        pass
    return(df)
    
def update():
    for sneaker in sneakers.items():
        df = scrape(sneaker[1])
        path = 'sneaker_sales/' + sneaker[0] + '.csv'
        
        if os.path.isfile(path):
            OGdf = pd.read_csv(path)
            
            if not OGdf.empty:
                df = pd.concat([df, OGdf], join='inner', ignore_index=True, axis=0)
                df.drop_duplicates(keep='first', inplace=True)
                df['createdAt'] = df['createdAt'].replace('T', ' ')
                df['createdAt'] = pd.to_datetime(df['createdAt'], format='%Y%m%d %H:%M') #infer_datetime_format=True
                df.sort_values(by='createdAt', ascending=False)
                df.index = range(0, len(df))
                
        df.to_csv(path)

if __name__ == '__main__':
    update()